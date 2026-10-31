// Controls display reads only. Runtime workers and terminal observers stay alive.
export function createVisibleStatusPoll({ refresh, activeView = true, observationRequired = false,
  intervalMs = 5000, visibility = document, timers = window, invalidate = () => {} }) {
  let timer = null;
  let flight = null;
  let disposed = false;
  let view = activeView;
  let required = observationRequired;
  const visible = () => visibility.visibilityState !== "hidden";
  const wanted = () => required || (view && visible());
  function read() {
    if (disposed || !wanted() || flight) return flight;
    flight = Promise.resolve().then(refresh).catch(() => {}).finally(() => { flight = null; });
    return flight;
  }
  function sync(catchUp = false) {
    if (disposed) return;
    if (!wanted()) {
      if (timer !== null) timers.clearInterval(timer);
      timer = null;
      invalidate();
      return;
    }
    if (timer === null) {
      timer = timers.setInterval(read, Math.max(1000, intervalMs));
      invalidate();
      void read();
    } else if (catchUp) {
      invalidate();
      void read();
    }
  }
  const visibilityChanged = () => sync(visible() && view);
  visibility.addEventListener("visibilitychange", visibilityChanged);
  sync();
  return {
    update(next) {
      const returnedToView = !view && next.activeView;
      view = next.activeView;
      required = next.observationRequired;
      sync(returnedToView && visible());
    },
    dispose() {
      disposed = true;
      if (timer !== null) timers.clearInterval(timer);
      timer = null;
      visibility.removeEventListener("visibilitychange", visibilityChanged);
    },
  };
}
