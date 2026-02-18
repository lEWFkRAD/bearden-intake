async function fetchJSON(url){
  const r = await fetch(url, {headers: {"Accept":"application/json"}});
  if(!r.ok) throw new Error(await r.text());
  return await r.json();
}

function setText(id, value){
  const el = document.getElementById(id);
  if(el) el.textContent = value;
}

function setPill(id, state, text){
  const el = document.getElementById(id);
  if(!el) return;
  el.classList.remove("good","warn","bad");
  el.classList.add(state);
  el.textContent = text;
}

async function pollAdmin(){
  try{
    const data = await fetchJSON("/api/admin/summary");
    setPill("health_pill", data.health.state, data.health.label);
    setText("kpi_jobs_today", data.kpis.jobs_today);
    setText("kpi_fail_today", data.kpis.failures_today);
    setText("kpi_avg_runtime", data.kpis.avg_runtime_s + "s");
    setText("kpi_ttfv", data.kpis.time_to_first_values_s + "s");
    setText("kpi_disk_free", data.kpis.disk_free_gb + " GB");
    setText("meta_version", data.health.version);
    setText("meta_uptime", data.health.uptime_h + "h");
  }catch(e){
    setPill("health_pill", "bad", "Disconnected");
  }

  // CAS: Poll CAS health if on a CAS page
  try{
    const casEl = document.getElementById("cas_overall_pill");
    if(casEl){
      const cas = await fetchJSON("/api/cas/health");
      setPill("cas_overall_pill", cas.state, cas.label);
    }
  }catch(e){
    // CAS health poll failure is non-critical
  }

  // T-CAS-2B: Poll open CR count if pill exists
  try{
    const crPill = document.getElementById("cr_count_pill");
    if(crPill){
      const crs = await fetchJSON("/api/cas/cr?status=open");
      const count = (crs.change_requests || []).length;
      crPill.textContent = count + " open";
      crPill.className = "pill " + (count > 0 ? "bad" : "good");
    }
  }catch(e){
    // CR poll failure is non-critical
  }
}

setInterval(pollAdmin, 7000);
window.addEventListener("load", pollAdmin);