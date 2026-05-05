document.body.addEventListener("htmx:afterSwap", () => {
  const consoles = document.querySelectorAll(".console");
  consoles.forEach((consoleEl) => {
    consoleEl.scrollTop = consoleEl.scrollHeight;
  });
});
