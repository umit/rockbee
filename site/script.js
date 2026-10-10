"use strict";

// Progressive enhancement: all content and links remain usable without JavaScript.
const descriptions = {
  put: "The writer persists your bytes through SlateDB. A successful response means the write is durable; readers may still be catching up.",
  get: "The gateway selects a reader round-robin. Its SlateDB state and RAM cache serve the lookup, fetching from object storage as needed. The value may lag recent writes.",
  delete: "The writer persists the deletion before acknowledging it. Readers can still return the previous value until they catch up with the deletion."
};

const diagram = document.querySelector(".architecture");
const flowButtons = document.querySelectorAll("[data-flow]");
document.querySelector(".flow-controls").hidden = false;
flowButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const flow = button.dataset.flow;
    diagram.dataset.active = flow;
    document.querySelector("#flow-method").textContent = flow.toUpperCase();
    document.querySelector("#flow-description").textContent = descriptions[flow];
    flowButtons.forEach((item) => item.setAttribute("aria-pressed", String(item === button)));
  });
});

if (navigator.clipboard && window.isSecureContext) {
  document.querySelectorAll("[data-copy]").forEach((button) => {
    button.hidden = false;
    button.addEventListener("click", async () => {
      const source = document.getElementById(button.dataset.copy);
      const status = document.getElementById("copy-status");
      try {
        await navigator.clipboard.writeText(source.textContent);
        button.textContent = "Copied";
        status.textContent = "Copied to clipboard.";
      } catch {
        button.textContent = "Select text";
        status.textContent = "Clipboard unavailable. Select and copy the code manually.";
      }
      window.setTimeout(() => { button.textContent = "Copy"; }, 2200);
    });
  });
}
