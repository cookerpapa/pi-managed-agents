-- Resource DB candidate. The serving workspace_compute_live_unique excludes released rows.
create index workspace_compute_released_scope on workspace_runtimes
  (tenant_id,workspace_id,sandbox_domain_id,compute_session_id) where state='released';
