#!/bin/sh
# Run as postgres inside the disposable study Pod, on a second, socket-only
# instance. pg_ctl always addresses the data directory just created here.
set -eu
umask 077
pma_fault_dir=$(mktemp -d /tmp/pma-async-crash-XXXXXXXX)
pma_fault_data="$pma_fault_dir/data"
cleanup() {
  if test -s "$pma_fault_data/postmaster.pid"; then
    pg_ctl -D "$pma_fault_data" -m immediate -w -t 20 stop >/dev/null
  fi
  rm -rf -- "$pma_fault_dir"
}
trap cleanup EXIT
query() {
  psql -X -qAt -h "$pma_fault_dir" -p 55432 -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}
start_server() {
  pg_ctl -D "$pma_fault_data" -l "$pma_fault_dir/server.log" -w -t 30 start >/dev/null
  test "$(query -c 'show data_directory')" = "$pma_fault_data"
}
initdb -D "$pma_fault_data" --auth-local=trust --auth-host=reject --no-locale >"$pma_fault_dir/init.log"
cat >>"$pma_fault_data/postgresql.conf" <<EOF
listen_addresses = ''
unix_socket_directories = '$pma_fault_dir'
unix_socket_permissions = 0700
port = 55432
fsync = on
synchronous_commit = on
EOF
start_server
test "$(query -c 'show wal_writer_delay')" = '200ms'
test "$(query -c 'show wal_writer_flush_after')" = '1MB'
query -c 'create table receipts(trial int, item int, primary key(trial,item))'
query -c 'insert into receipts values (0,0)'
trial=0
for mode in on off off on on off; do
  trial=$((trial+1))
  query -c 'checkpoint'
  {
    echo "set synchronous_commit='$mode';"
    echo "select 'settings|'||current_setting('synchronous_commit')||'|'||current_setting('fsync');"
    i=1
    while test "$i" -le 32; do
      echo "insert into receipts values ($trial,$i) returning 'ack|'||item;"
      i=$((i+1))
    done
    echo "select 'gap|'||pg_wal_lsn_diff(pg_current_wal_insert_lsn(),pg_current_wal_flush_lsn());"
  } >"$pma_fault_dir/input.sql"
  # psql autofinishes each separate command before advancing to the next one.
  # The client's receipts live outside PGDATA and survive the server crash.
  query -f "$pma_fault_dir/input.sql" >"$pma_fault_dir/acks"
  pg_ctl -D "$pma_fault_data" -m immediate -w -t 20 stop >/dev/null
  start_server
  grep -Fxq "settings|$mode|on" "$pma_fault_dir/acks"
  ack=$(awk -F'|' '$1=="ack"{n++; if($2!=n)exit 1} END{print n+0}' "$pma_fault_dir/acks")
  test "$ack" = 32
  gap=$(awk -F'|' '$1=="gap"{print $2}' "$pma_fault_dir/acks")
  test "$(query -c 'select count(*) from receipts where trial=0 and item=0')" = 1
  query -c "select item from receipts where trial=$trial order by item" >"$pma_fault_dir/recovered"
  recovered=$(awk '$1!=NR{exit 1} END{print NR+0}' "$pma_fault_dir/recovered")
  test "$recovered" -le "$ack"
  if test "$mode" = on; then test "$recovered" -eq "$ack"; fi
  printf '{"trial":%s,"mode":"%s","acknowledged":%s,"recovered":%s,"lost":%s,"gapBytesBeforeStop":%s}\n' \
    "$trial" "$mode" "$ack" "$recovered" "$((ack-recovered))" "$gap"
done
