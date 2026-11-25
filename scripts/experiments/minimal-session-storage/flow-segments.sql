-- An immutable directory per writable flow. Source flows can outlive their windows.
create table flows (
  tenant_id uuid not null,
  flow_id uuid not null,
  primary key (tenant_id, flow_id)
);
create table flow_segments (
  tenant_id uuid not null,
  destination_flow uuid not null,
  segment_no integer not null check (segment_no > 0),
  source_flow uuid not null,
  conversation_id bigint not null,
  first_entry bigint not null,
  last_entry bigint not null check (last_entry >= first_entry),
  through_seq bigint not null check (through_seq > 0),
  through_position bigint not null check (through_position > 0),
  tail_coverage bigint, -- a partial Turn has no inherited terminal event
  primary key (tenant_id, destination_flow, segment_no),
  unique (tenant_id, destination_flow, source_flow, conversation_id),
  check (destination_flow <> source_flow),
  foreign key (tenant_id, destination_flow) references flows on delete cascade,
  foreign key (tenant_id, source_flow) references flows
);
create index flow_segment_source on flow_segments(tenant_id,source_flow,conversation_id,last_entry);

-- Caller verifies the tenant, source window, selected Entry and the anchor policy.
-- The new flow is private until its owning session and pins commit together.
create function freeze_flow_segments(t uuid, src uuid, dest uuid, physical uuid, lane bigint,
  anchor bigint, ceiling bigint, mailbox bigint, partial_coverage bigint) returns void language plpgsql as $$
declare boundary flow_segments%rowtype;
begin
  if src=dest or exists(select from flow_segments where tenant_id=t and destination_flow=dest) then
    raise exception 'Fork destination is not empty';
  end if;
  if physical=src then
    insert into flow_segments
      select tenant_id,dest,segment_no,source_flow,conversation_id,first_entry,last_entry,through_seq,through_position,tail_coverage
      from flow_segments where tenant_id=t and destination_flow=src;
    insert into flow_segments
      select t,dest,coalesce(max(segment_no),0)+1,src,lane,1,anchor,ceiling,mailbox,partial_coverage
      from flow_segments where tenant_id=t and destination_flow=dest;
  else
    select * into strict boundary from flow_segments where tenant_id=t and destination_flow=src
      and source_flow=physical and conversation_id=lane and anchor between first_entry and last_entry;
    insert into flow_segments
      select tenant_id,dest,segment_no,source_flow,conversation_id,first_entry,
        case when segment_no=boundary.segment_no then anchor else last_entry end,
        case when segment_no=boundary.segment_no then least(ceiling,through_seq) else through_seq end,
        case when segment_no=boundary.segment_no then mailbox else through_position end,
        case when segment_no<>boundary.segment_no then tail_coverage when partial_coverage is not null then partial_coverage
          when mailbox=boundary.through_position then tail_coverage end
      from flow_segments where tenant_id=t and destination_flow=src and segment_no<=boundary.segment_no;
  end if;
end $$;
