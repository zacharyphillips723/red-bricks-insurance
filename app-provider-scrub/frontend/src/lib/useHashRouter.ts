import { useState, useEffect, useCallback } from "react";

/**
 * Drop-in replacement for useState that syncs page state with the URL hash.
 * Enables browser back/forward navigation and shareable URLs with zero dependencies.
 */
export function useHashRouter<T extends string = string>(defaultPage: T): [T, (page: T) => void] {
  const readHash = (): T => {
    const hash = window.location.hash.replace(/^#\/?/, "");
    return (hash || defaultPage) as T;
  };

  const [page, setPageState] = useState<T>(readHash);

  const setPage = useCallback((newPage: T) => {
    window.location.hash = `#/${newPage}`;
  }, []);

  useEffect(() => {
    const onHashChange = () => setPageState(readHash());
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  useEffect(() => {
    if (!window.location.hash) {
      window.history.replaceState(null, "", `#/${defaultPage}`);
    }
  }, []);

  return [page, setPage];
}
