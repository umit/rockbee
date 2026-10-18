(() => {
  "use strict";

  const traces = {
    put: {
      kicker: "TABLE PUT · DURABLE PATH",
      title: "A table write is acknowledged after SlateDB durability",
      note: "Asynchronous Parquet publication is independent of the PUT acknowledgement; a durable ACK does not require export completion or establish an ordering with it.",
      steps: [
        {
          title: "Create the table mutation",
          body: "The client validates the table name, ID, and JSON object before it admits the write.",
          nodes: ["client"], edges: [], language: "rust",
          code: "let products = client.table(\"products\");\nlet write = products.put_key(\n    \"product-42\",\n    &json!({\"name\": \"Trail Backpack\", \"price\": 79}),\n    None,\n);"
        },
        {
          title: "Reserve bounded admission",
          body: "PUT returns its handle immediately after local admission. The handle can be awaited later; dropping it does not cancel an admitted write.",
          nodes: ["client"], edges: ["client-gateway"], language: "rust",
          code: "// The write has started; its handle is eager.\nwrite.await?; // Ok(()) is the durable writer acknowledgement"
        },
        {
          title: "Route the mutation once",
          body: "The gateway validates the request and sends table PUT to the single writer. It does not replay a mutation when a reply is absent.",
          nodes: ["gateway", "writer"], edges: ["client-gateway", "gateway-writer"], language: "rust",
          code: "// Gateway routes table PUT to Writer.\n// An uncertain mutation is never replayed automatically."
        },
        {
          title: "Write the document and catalogue",
          body: "The writer atomically writes the document plus a persistent table-catalogue entry to SlateDB.",
          nodes: ["writer", "store"], edges: ["gateway-writer", "writer-store"], language: "rust",
          code: "// Writer implementation: one SlateDB batch contains\n// the table row and its persistent table-catalogue entry."
        },
        {
          title: "Receive the durable acknowledgement",
          body: "Only after SlateDB's durability handle completes does the writer return OK. That makes the PUT durable, not immediately visible on a reader.",
          nodes: ["writer", "store", "gateway", "client"], edges: ["writer-store", "gateway-writer", "client-gateway"], language: "rust",
          code: "write.await?;\n// Ok(()) means the writer acknowledged durability."
        },
        {
          title: "Background SQL publication",
          body: "A background writer-owned exporter can turn table changes into immutable Parquet and a manifest. A change becomes visible to SQL when this separate path publishes it.",
          nodes: ["writer", "exporter", "parquet"], edges: ["writer-exporter", "exporter-parquet"], language: "rust",
          code: "// Background publication is independent of this PUT handle.\n// A later manifest can make this change queryable."
        }
      ]
    },
    get: {
      kicker: "TABLE GET · EVENTUAL READER PATH",
      title: "A reader serves the value it has caught up to",
      note: "A durable writer acknowledgement does not provide immediate reader visibility. A missing value is still a successful reader response.",
      steps: [
        { title: "Ask for one document", body: "The client validates the table name and ID, then starts a read on its shared bounded connection.", nodes: ["client"], edges: [], language: "rust", code: "let document = products.get(\"product-42\").await?;" },
        { title: "Route to a reader", body: "The gateway round-robins GET requests across readers; it does not send this lookup to the writer.", nodes: ["gateway", "reader"], edges: ["client-gateway", "gateway-reader"], language: "rust", code: "// read route: Gateway -> Reader" },
        { title: "Use the reader's SlateDB view", body: "The reader opens SlateDB read-only and follows durable writer updates asynchronously.", nodes: ["reader", "store"], edges: ["gateway-reader", "reader-store"], language: "rust", code: "// Reader looks up the physical table key in its SlateDB view." },
        { title: "Filter an expired value", body: "The reader uses its wall clock to hide a value at or after its stored expiry time.", nodes: ["reader", "store"], edges: ["reader-store"], language: "rust", code: "// Compare the stored expiry with the reader clock.\n// Return Some(document) or None." },
        { title: "Return the reader result", body: "The gateway returns this reader's result, including None. Poll again when eventual convergence is acceptable.", nodes: ["reader", "gateway", "client"], edges: ["gateway-reader", "client-gateway"], language: "rust", code: "match document {\n    Some(value) => println!(\"{value:?}\"),\n    None => println!(\"reader has no visible value\"),\n}" }
      ]
    },
    sql: {
      kicker: "TABLE SQL · PUBLISHED PARQUET PATH",
      title: "SQL reads a published Parquet generation",
      note: "SQL is eventual and may lag both the writer and point GET. The returned export watermark is diagnostic, not a mutation receipt or read barrier.",
      steps: [
        { title: "Submit one SELECT", body: "The client validates nonempty SQL up to 64 KiB and sends it verbatim as a table query.", nodes: ["client"], edges: [], language: "sql", code: "SELECT id, value->>'name' AS name\nFROM products\nORDER BY id\nLIMIT 10" },
        { title: "Route SQL to a reader", body: "The gateway treats table SQL as a read and routes it to a reader, never to the writer.", nodes: ["gateway", "reader"], edges: ["client-gateway", "gateway-reader"], language: "sql", code: "-- query route: Gateway -> Reader" },
        { title: "Load the table manifest", body: "For every query, the reader reloads the latest immutable manifest to choose the authoritative Parquet generation.", nodes: ["reader", "parquet"], edges: ["reader-parquet"], language: "rust", code: "// Reader reloads the latest manifest before each query.\n// Its immutable segment list defines the selected generation." },
        { title: "Fetch immutable files to the local cache", body: "The reader downloads manifest-listed immutable segments into its own bounded cache; cache contents do not decide which generation is authoritative.", nodes: ["reader", "parquet"], edges: ["reader-parquet"], language: "rust", code: "// Download only manifest-listed immutable segments\n// into this reader's local cache." },
        { title: "Run DuckDB over the selected generation", body: "This illustrative internal view uses a local cached filename and a sample query-time timestamp. It keeps the newest row per ID, then filters tombstones and expiry before the user predicate runs.", nodes: ["reader", "duckdb", "parquet"], edges: ["reader-duckdb", "parquet-duckdb"], language: "sql", code: "SELECT id, CAST(value AS JSON) AS value\nFROM (\n  SELECT *, row_number() OVER (\n    PARTITION BY id ORDER BY seq DESC, wal_file_id DESC, row_ordinal DESC\n  ) AS _rockbee_rank\n  FROM read_parquet(['cached-segment.parquet'], union_by_name=true)\n)\nWHERE _rockbee_rank = 1 AND NOT tombstone\n  AND (expires_at_unix_ms IS NULL OR expires_at_unix_ms > 1760000000000)" },
        { title: "Return bounded JSON rows", body: "The reader returns JSON objects with selected column names, bounded to 1,000 rows and 512 KiB.", nodes: ["duckdb", "reader", "gateway", "client"], edges: ["reader-duckdb", "gateway-reader", "client-gateway"], language: "rust", code: "let result = products.query(sql).await?;\n// result.exported_wal_file_id is an export watermark" }
      ]
    },
    delete: {
      kicker: "TABLE DELETE · DURABLE TOMBSTONE",
      title: "Delete is durable before readers and SQL catch up",
      note: "A deleted document may remain visible through a lagging reader or an older published SQL generation until those independent paths converge. Old Parquet objects are retained today.",
      steps: [
        { title: "Start the delete", body: "The client validates the table name and ID before it admits the mutation.", nodes: ["client"], edges: [], language: "rust", code: "products.delete(\"product-42\").await?;" },
        { title: "Route once to the writer", body: "The gateway sends the delete to the writer and never automatically replays an uncertain mutation.", nodes: ["gateway", "writer"], edges: ["client-gateway", "gateway-writer"], language: "rust", code: "// mutation route: Gateway -> Writer" },
        { title: "Persist the SlateDB tombstone", body: "The writer deletes the table key and waits for SlateDB durability before replying success.", nodes: ["writer", "store"], edges: ["gateway-writer", "writer-store"], language: "rust", code: "// Writer implementation persists a SlateDB tombstone\n// and waits for its durability handle before returning OK." },
        { title: "Receive the durable ACK", body: "The client now knows the writer made the deletion durable. It does not yet know that every reader or SQL projection has observed it.", nodes: ["writer", "gateway", "client"], edges: ["gateway-writer", "client-gateway"], language: "rust", code: "// Ok(()) means durable writer acknowledgement" },
        { title: "Readers converge independently", body: "A reader later follows the durable update and returns None for the deleted document.", nodes: ["reader", "store"], edges: ["reader-store"], language: "rust", code: "products.get(\"product-42\").await? // eventually None" },
        { title: "Publish the SQL tombstone later", body: "The exporter publishes a later manifest generation. DuckDB filters the newest tombstone; previous immutable Parquet objects remain retained.", nodes: ["exporter", "parquet", "duckdb"], edges: ["writer-exporter", "exporter-parquet", "parquet-duckdb"], language: "sql", code: "WHERE _rockbee_rank = 1\n  AND NOT tombstone" }
      ]
    }
  };

  const byId = (id) => document.getElementById(id);
  const selectAll = (selector) => Array.from(document.querySelectorAll(selector));

  function initTrace() {
    const scenarioButtons = selectAll("[data-trace]");
    const stepsElement = byId("trace-steps");
    if (!scenarioButtons.length || !stepsElement) return;

    const count = byId("trace-count");
    const kicker = byId("trace-kicker");
    const title = byId("trace-title");
    const body = byId("trace-body");
    const code = byId("trace-code");
    const note = byId("trace-note");
    const previous = byId("trace-prev");
    const next = byId("trace-next");
    const reset = byId("trace-reset");
    let scenario = scenarioButtons.find((button) => button.getAttribute("aria-pressed") === "true")?.dataset.trace || "put";
    let stepIndex = 0;

    function render() {
      const trace = traces[scenario];
      const step = trace.steps[stepIndex];
      if (count) count.textContent = `${stepIndex + 1} / ${trace.steps.length}`;
      if (kicker) kicker.textContent = trace.kicker;
      if (title) title.textContent = step.title || trace.title;
      if (body) body.textContent = step.body;
      if (note) note.textContent = trace.note;
      if (code) {
        code.textContent = step.code;
        code.className = `language-${step.language}`;
      }
      scenarioButtons.forEach((button) => button.setAttribute("aria-pressed", String(button.dataset.trace === scenario)));
      selectAll("[data-node]").forEach((node) => {
        const index = trace.steps.findIndex((item) => item.nodes.includes(node.dataset.node));
        node.classList.toggle("is-active", step.nodes.includes(node.dataset.node));
        node.classList.toggle("is-visited", index >= 0 && index < stepIndex);
      });
      selectAll("[data-edge]").forEach((edge) => edge.classList.toggle("is-active", step.edges.includes(edge.dataset.edge)));

      const focusedStep = stepsElement.contains(document.activeElement)
        ? Number(document.activeElement.dataset.traceStep)
        : null;
      stepsElement.replaceChildren(...trace.steps.map((item, index) => {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = String(index + 1);
        button.dataset.traceStep = String(index);
        button.setAttribute("aria-label", `Step ${index + 1}: ${item.title}`);
        button.setAttribute("aria-current", index === stepIndex ? "step" : "false");
        button.classList.toggle("is-active", index === stepIndex);
        button.addEventListener("click", () => { stepIndex = index; render(); });
        return button;
      }));
      if (Number.isInteger(focusedStep)) {
        stepsElement.querySelector(`[data-trace-step=\"${Math.min(focusedStep, trace.steps.length - 1)}\"]`)?.focus();
      }
      if (previous) previous.disabled = stepIndex === 0;
      if (next) next.disabled = stepIndex === trace.steps.length - 1;
    }

    scenarioButtons.forEach((button) => button.addEventListener("click", () => {
      scenario = button.dataset.trace;
      stepIndex = 0;
      render();
    }));
    previous?.addEventListener("click", () => { if (stepIndex > 0) { stepIndex -= 1; render(); } });
    next?.addEventListener("click", () => { if (stepIndex < traces[scenario].steps.length - 1) { stepIndex += 1; render(); } });
    reset?.addEventListener("click", () => { stepIndex = 0; render(); });
    render();
  }

  function initFreshness() {
    const actions = selectAll("[data-fresh-action]");
    if (!actions.length) return;
    const initial = { revision: 1, exists: true, price: 79, lastPrice: 79 };
    let writer;
    let reader;
    let published;

    function clone(snapshot) { return { ...snapshot }; }
    function same(left, right) { return left.revision === right.revision && left.exists === right.exists && left.price === right.price; }
    function valueFor(prefix, snapshot) {
      if (snapshot.exists) return JSON.stringify({ name: "Trail Backpack", price: snapshot.price });
      return prefix === "writer" ? "deleted" : prefix === "reader" ? "None" : "[]";
    }
    function stateFor(prefix, snapshot) {
      if (prefix !== "writer" && !same(snapshot, writer)) return "lagging";
      return snapshot.exists ? "fresh" : "deleted";
    }
    function renderCard(prefix, snapshot) {
      const card = byId(`${prefix}-card`);
      const state = stateFor(prefix, snapshot);
      const version = byId(`${prefix}-version`);
      const value = byId(`${prefix}-value`);
      const label = byId(`${prefix}-state`);
      if (version) version.textContent = `revision ${snapshot.revision}`;
      if (value) value.textContent = valueFor(prefix, snapshot);
      if (label) label.textContent = state;
      if (card) card.dataset.state = state;
    }
    function message(text) { const element = byId("fresh-message"); if (element) element.textContent = text; }
    function render() { renderCard("writer", writer); renderCard("reader", reader); renderCard("sql", published); }
    function reset() { writer = clone(initial); reader = clone(initial); published = clone(initial); render(); message("Initial revision 1 is already durable, reader-visible, and published for SQL."); }

    actions.forEach((button) => button.addEventListener("click", () => {
      switch (button.dataset.freshAction) {
        case "write":
          writer = { revision: writer.revision + 1, exists: true, price: writer.lastPrice + 10, lastPrice: writer.lastPrice + 10 };
          render();
          message(`Writer acknowledged revision ${writer.revision} as durable. Reader and SQL remain independent freshness paths.`);
          break;
        case "delete":
          writer = { ...writer, revision: writer.revision + 1, exists: false };
          render();
          message(`Writer acknowledged a durable tombstone at revision ${writer.revision}. Lagging reader and SQL views can still show older data.`);
          break;
        case "read":
          reader = clone(writer);
          render();
          message(`Reader caught up to revision ${reader.revision}. SQL publication remains independent after the table catalogue is ready.`);
          break;
        case "publish":
          published = clone(writer);
          render();
          message(`Published Parquet now represents revision ${published.revision}. This publication is separate from reader catch-up.`);
          break;
        case "reset":
          reset();
          break;
      }
    }));
    reset();
  }

  function initComparison() {
    const buttons = selectAll("[data-compare]");
    if (!buttons.length) return;
    const select = (choice) => {
      buttons.forEach((button) => button.setAttribute("aria-pressed", String(button.dataset.compare === choice)));
      selectAll("[data-comparison]").forEach((section) => { section.hidden = section.dataset.comparison !== choice; });
    };
    buttons.forEach((button) => button.addEventListener("click", () => select(button.dataset.compare)));
    select(buttons.find((button) => button.getAttribute("aria-pressed") === "true")?.dataset.compare || buttons[0].dataset.compare);
  }

  function initScale() {
    const input = byId("reader-count");
    if (!input) return;
    const output = byId("reader-output");
    const note = byId("scale-note");
    const readers = selectAll("[data-reader-index]");
    const render = () => {
      const count = Math.max(1, Math.min(6, Number(input.value) || 1));
      if (output) output.textContent = `${count} reader${count === 1 ? "" : "s"}`;
      readers.forEach((reader) => { reader.hidden = Number(reader.dataset.readerIndex) > count; });
      if (note) note.textContent = `${count} reader${count === 1 ? " contributes" : "s contribute"} separate serving resources and cache capacity; throughput depends on the workload and is not guaranteed to scale linearly.`;
    };
    input.addEventListener("input", render);
    render();
  }

  function init() { initTrace(); initFreshness(); initComparison(); initScale(); }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
