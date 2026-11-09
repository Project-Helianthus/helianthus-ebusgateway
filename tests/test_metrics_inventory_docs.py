"""Offline source contract for the composed metrics route; no scrape is implied."""
import copy
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
PAGE = "docs/observability/semantic-prometheus-coverage.md"
SOURCES = [
    "bus_observability_store.go", "runtime_transport_metrics.go",
    "baud_observation_metrics.go", "semantic_prometheus.go",
    "internal/semanticmetrics/metrics.go", "cmd/gateway/huawei_transport_metrics.go",
    "cmd/gateway/semantic_metrics_runtime.go", "cmd/gateway/gateway_http_server.go",
    "cmd/gateway/gateway_http_control_plane.go", "semantic_prometheus_text.go",
]

# Parse Go syntax rather than treating literal name matches as an output graph.
# The dynamic writer and format sites below are resolved explicitly, fail closed.
ANALYZER = r'''
package main
import("bytes"; "encoding/json"; "go/ast"; "go/format"; "go/parser"; "go/token"; "os"; "strconv")
type Call struct { Source, Function, Method string; Args []string; Guards []string }
type Result struct { Calls []Call; Fields map[string]string; Expvar map[string]string }
func main() {
 r:=Result{Fields:map[string]string{}, Expvar:map[string]string{}}
 for _,path:=range os.Args[1:] {
  fs:=token.NewFileSet(); f,e:=parser.ParseFile(fs,path,nil,0); if e!=nil { panic(e) }
  text:=func(n ast.Node) string { var b bytes.Buffer; format.Node(&b,fs,n); return b.String() }
  ast.Inspect(f,func(n ast.Node) bool {
   v,ok:=n.(*ast.ValueSpec); if !ok {return true}
   for _,value:=range v.Values { cl,ok:=value.(*ast.CompositeLit); if !ok || len(v.Names)==0 || v.Names[0].Name!="metrics" {continue}
    for _,elt:=range cl.Elts { row,ok:=elt.(*ast.CompositeLit); if !ok || len(row.Elts)<2 {panic("uncovered metric table shape")}; a,_:=strconv.Unquote(text(row.Elts[0])); b,_:=strconv.Unquote(text(row.Elts[1])); if a=="" || b=="" {panic("dynamic table entry")}; r.Fields[a]=b }
   }; return true
  })
  for _,decl:=range f.Decls { fn,ok:=decl.(*ast.FuncDecl); if !ok {continue}
   stack:=[]ast.Node{}
   ast.Inspect(fn.Body,func(n ast.Node) bool {
    if n==nil {stack=stack[:len(stack)-1]; return false}; stack=append(stack,n)
    c,ok:=n.(*ast.CallExpr); if !ok {return true}; method:=text(c.Fun)
    args:=[]string{}; for _,a:=range c.Args {args=append(args,text(a))}
    guards:=[]string{}; for _,parent:=range stack {if branch,ok:=parent.(*ast.IfStmt); ok {guards=append(guards,text(branch.Cond))}}
    r.Calls=append(r.Calls,Call{path,fn.Name.Name,method,args,guards}); return true
   })
  }
  ast.Inspect(f,func(n ast.Node) bool {c,ok:=n.(*ast.CallExpr); if !ok {return true}; s,ok:=c.Fun.(*ast.SelectorExpr); if !ok {return true}; id,ok:=s.X.(*ast.Ident); if !ok || id.Name!="expvar" || len(c.Args)!=1 {return true}; name,e:=strconv.Unquote(text(c.Args[0])); if e==nil {r.Expvar[name]=s.Sel.Name}; return true})
 }
 json.NewEncoder(os.Stdout).Encode(r)
}
'''


def literal(expr):
    return json.loads(expr)


def expected_families(analysis):
    """Resolve literal writer sites and the two finite dynamic renderers."""
    families = {}
    samples = {}
    guards = {}
    for c in analysis["Calls"]:
        method, args = c["Method"], c["Args"]
        if method.endswith(".writeType"):
            name, kind = map(literal, args)
            assert name not in families, f"duplicate declaration: {name}"
            families[name] = dict(type=kind, source=c["Source"])
        if method.endswith((".writeGaugeSample", ".writeCounterSample")) or method == "emit":
            if args[0] == "name":
                assert (c["Source"], c["Function"], method, args) == (
                    "semantic_prometheus.go", "writeSemanticMetrics", "w.writeGaugeSample",
                    ["name", "value", "labels"]), "uncovered dynamic writer"
                continue
            name = literal(args[0])
            samples.setdefault(name, []).append(args[2])
            guards.setdefault(name, set()).update(c["Guards"])
    assert set(samples) == set(families), "sample without a declared family"
    for name, row in families.items():
        assert name in samples, f"declaration without resolved sample: {name}"
        row["source_guards"] = sorted(guards[name])
        row["labels"] = sorted(set().union(*(sample_labels(x, name) for x in samples[name])))
    for field, name in analysis["Fields"].items():
        assert name not in families
        families[name] = dict(type="gauge", source="internal/semanticmetrics/metrics.go", labels=["slot"], field=field)
    families["helianthus_semantic_field_available"] = dict(
        type="gauge", source="internal/semanticmetrics/metrics.go", labels=["domain", "field", "slot"])
    families["helianthus_huawei_transport_events_total"] = dict(
        type="counter", source="cmd/gateway/huawei_transport_metrics.go", labels=["family", "function", "outcome"])
    return families


def semantic_fact_base_labels():
    """Read the fixed label keys from the production helper body."""
    text = (ROOT / "semantic_prometheus.go").read_text()
    start = text.index("func semanticFactLabels(")
    body = text[start:text.index("\nfunc ", start + 1)]
    call = re.search(r"labelMap\(([^\n]*)\)", body)
    assert call, "semanticFactLabels no longer builds labels with labelMap"
    keys = {literal(x) for x in split_top_level(call.group(1))[::2] if x.startswith('"')}
    keys |= set(re.findall(r'labels\["([a-z_]+)"\]\s*=', body))
    assert {"domain", "pack", "fact_id", "dimension"} <= keys, "unexpected semanticFactLabels shape"
    return keys


def split_top_level(value):
    result, start, depth, quoted, escaped = [], 0, 0, False, False
    for i, char in enumerate(value):
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "({[":
            depth += 1
        elif char in ")}]":
            depth -= 1
        elif char == "," and depth == 0:
            result.append(value[start:i].strip()); start = i + 1
    if value[start:].strip():
        result.append(value[start:].strip())
    return result


def sample_labels(expr, name):
    if expr == "nil":
        return set()
    # Label arguments are Go expressions; commas inside nested calls are ignored.
    def split_args(value):
        result, start, depth, quoted, escaped = [], 0, 0, False, False
        for i, char in enumerate(value):
            if quoted:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
            elif char in "({[":
                depth += 1
            elif char in ")}]":
                depth -= 1
            elif char == "," and depth == 0:
                result.append(value[start:i].strip()); start = i + 1
        if value[start:].strip():
            result.append(value[start:].strip())
        return result
    if expr.startswith("labelMap("):
        args = split_args(expr[len("labelMap("):-1])
        assert len(args) % 2 == 0
        return {literal(x) for x in args[::2]}
    base = semantic_fact_base_labels()
    if expr == "labels":
        assert name == "helianthus_semantic_fact_state"
        return base | {"qualification", "promotion", "quality", "availability", "freshness"}
    assert expr.startswith("semanticFactLabels("), f"uncovered label builder: {expr}"
    args = split_args(expr[len("semanticFactLabels("):-1])
    return base | {literal(x) for x in args[4::2]}


@pytest.fixture(scope="module")
def analysis():
    scratch = ROOT / ".tmp"
    scratch.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as directory:
        helper = Path(directory) / "inspect.go"
        helper.write_text(ANALYZER)
        env = dict(os.environ, GO111MODULE="off", GOWORK="off", GOPROXY="off", GOSUMDB="off", GOTOOLCHAIN="local", GOFLAGS="-buildvcs=false")
        # Compile the stdlib-only parser independently of gateway dependencies.
        exe = Path(directory) / "inspect"
        built = subprocess.run(["go", "build", "-o", str(exe), str(helper)], cwd=ROOT, env=env, capture_output=True, text=True, timeout=90)
        assert built.returncode == 0, built.stderr
        parsed = subprocess.run([str(exe), *SOURCES, "cmd/gateway/semantic_vaillant_adapter_info.go"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=30)
        assert parsed.returncode == 0, parsed.stderr
        return json.loads(parsed.stdout)


def validate(inventory, page, analysis):
    expected = expected_families(analysis)
    rows = inventory["families"]
    assert len(rows) == len({r["name"] for r in rows}), "duplicate family"
    assert {r["name"] for r in rows} == set(expected), "rendered family inventory mismatch"
    for row in rows:
        for key, value in expected[row["name"]].items():
            assert row[key] == value, f"{row['name']}: wrong {key}"
        for key in ("enablement", "omission", "production", "owner_page"):
            assert row.get(key), f"{row['name']}: missing {key}"
        assert row["owner_page"] == PAGE
        assert table_row(row) in page, f"documentation drift: {row['name']}"
    section = page.split("## Complete composed route inventory", 1)[1].split("## Adapter telemetry is expvar-only", 1)[0]
    actual_rows = {line for line in section.splitlines() if line.startswith("| `")}
    assert actual_rows == {table_row(row) for row in rows}, "unlisted documentation family"
    expvar = {k: v for k, v in analysis["Expvar"].items() if k.startswith("ebus_adapter_")}
    assert inventory["expvar_only"] == expvar, "false expvar promotion"
    assert not (set(expvar) & set(expected))
    assert inventory["renderer_calls"] == renderer_calls(analysis), "renderer composition changed; retrace output paths"
    for name in expvar:
        assert f"`{name}`" in page
    assert inventory["evidence"] == "Proven: source renderer support; no scrape or physical qualification inferred"


def table_row(row):
    labels = ", ".join(f"`{x}`" for x in row["labels"]) or "none"
    return f"| `{row['name']}` | {row['type']} | {labels} | {row['enablement']} | {row['omission']} | {row['production']} | [{Path(row['source']).name}](../../{row['source']}) |"


def test_inventory_and_documentation(analysis):
    validate(json.loads((ROOT / "docs/metrics-inventory.json").read_text()), (ROOT / PAGE).read_text(), analysis)


@pytest.mark.parametrize("mutation", ["missing_family", "expvar_promotion", "wrong_type", "missing_enablement", "wrong_labels", "missing_dynamic_field"])
def test_independent_negative_inventory_mutations(analysis, mutation):
    inventory = json.loads((ROOT / "docs/metrics-inventory.json").read_text())
    if mutation == "missing_family":
        inventory["families"] = [r for r in inventory["families"] if r["name"] != "ebus_frames_observed_total"]
    elif mutation == "expvar_promotion":
        inventory["families"].append(dict(inventory["families"][0], name="ebus_adapter_info_health"))
    elif mutation == "wrong_type":
        next(r for r in inventory["families"] if r["name"] == "ebus_frames_observed_total")["type"] = "gauge"
    elif mutation == "missing_enablement":
        del next(r for r in inventory["families"] if r["name"] == "ebus_bus_busy_ratio")["enablement"]
    elif mutation == "wrong_labels":
        next(r for r in inventory["families"] if r["name"] == "ebus_errors_total")["labels"] = []
    else:
        inventory["families"] = [r for r in inventory["families"] if r["name"] != "helianthus_thermal_action_state"]
    with pytest.raises((AssertionError, KeyError)):
        validate(inventory, (ROOT / PAGE).read_text(), analysis)


def test_dynamic_writer_fails_closed(analysis):
    changed = copy.deepcopy(analysis)
    changed["Calls"].append(dict(Source="semantic_prometheus.go", Function="writeSemanticMetrics", Method="w.writeGaugeSample", Args=["newName", "value", "labels"]))
    with pytest.raises((AssertionError, ValueError)):
        expected_families(changed)


# These output roots and finite dynamic sites are reviewed together. An added
# call requires retracing its renderer rather than silently scanning more names.
OUTPUT_ROOTS = {"RenderPrometheus", "writeTransportRuntimeMetrics", "writeBaudObservationMetrics", "writeSemanticMetrics", "renderSemanticFact", "Render", "text", "writeSample", "writeGaugeSample", "writeCounterSample", "writeType", "writeHelp", "newGatewayMetricsHandler", "MetricsHandler", "startHTTPServer", "registerHTTPControlPlaneCoreRoutes", "newHTTPControlPlaneRoutePlan", "RenderSemanticPrometheus"}


def renderer_calls(analysis):
    result = {}
    for call in analysis["Calls"]:
        if call["Function"] in OUTPUT_ROOTS:
            key = call["Source"] + ":" + call["Function"]
            result.setdefault(key, set()).add(call["Method"])
    return {key: sorted(value) for key, value in sorted(result.items())}


def test_finite_format_renderers_and_route_composition(analysis):
    calls = analysis["Calls"]
    formats = [(c["Source"], c["Function"], c["Args"]) for c in calls if c["Method"] == "fmt.Fprintf" and c["Source"] == "internal/semanticmetrics/metrics.go"]
    expected = [
        ["&availability", json.dumps("# HELP %s Whether a configured semantic field is current, admitted, qualified, valid and fresh (0 or 1).\n# TYPE %s gauge\n"), "availabilityName", "availabilityName"],
        ["&samples", json.dumps('%s{slot="%d"} %s\n'), "m.name", "b.Slot", "strconv.FormatFloat(value, 'g', -1, 64)"],
        ["&availability", json.dumps('%s{domain="%s",field="%s",slot="%d"} %d\n'), "availabilityName", "b.Domain", "m.field", "b.Slot", "available"],
        ["&values", json.dumps("# HELP %s %s\n# TYPE %s gauge\n%s"), "m.name", "m.help", "m.name", "samples.String()"],
    ]
    assert formats == [("internal/semanticmetrics/metrics.go", "Render", args) for args in expected]
    huawei = [c for c in calls if c["Method"] == "fmt.Sprintf" and c["Function"] == "text"]
    assert len(huawei) == 1
    assert huawei[0]["Args"] == [json.dumps('helianthus_huawei_transport_events_total{family=%q,function=%q,outcome=%q} %d\n'), "key.family", "function", "key.outcome", "total"]
    # Follow the entry point and both appended renderers. No expvar forwarding
    # is composed here; the distinct debug route uses expvar.Handler.
    evidence = {
        "cmd/gateway/gateway_http_server.go": ["newGatewayMetricsHandler(busObservability, semanticMetrics)", "newGatewaySemanticMetrics(sharedSemanticProvider, gatewaySemanticMetricRegistry())"],
        "cmd/gateway/semantic_metrics_runtime.go": ["transport := store.MetricsHandler()", "transport.ServeHTTP(w, r)", "semanticmetrics.Render(view, m.registry)", "io.WriteString(w, semantic.text(r.Context()))", "io.WriteString(w, huaweiWireCounters.text())", "return semanticmetrics.Registry{}"],
        "cmd/gateway/gateway_http_control_plane.go": ['mux.Handle("/debug/vars", expvar.Handler())', "mux.Handle(plan.metricsPath, metricsHandler)", "if hasBusObservability {"],
        "runtime_transport_metrics.go": ["writeBaudObservationMetrics(writer, ebusConfigured)"],
        "baud_observation_metrics.go": ["if !ebusConfigured {\n\t\treturn"],
        "internal/semanticmetrics/metrics.go": ['const availabilityName = "helianthus_semantic_field_available"', "for _, m := range metrics {", "if samples.Len() > 0 {"],
        "cmd/gateway/huawei_transport_metrics.go": ["if len(s.totals) == 0 {", "# TYPE helianthus_huawei_transport_events_total counter"],
        "semantic_prometheus_text.go": ["writeSemanticMetrics(newPrometheusWriter(&buffer), domains, now)"],
    }
    for path, snippets in evidence.items():
        source = (ROOT / path).read_text()
        for snippet in snippets:
            assert snippet in source, f"retrace composition: {path}: {snippet}"
