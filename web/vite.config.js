import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { createAppearanceApi } from "./src/appearance.js";

export default defineConfig({
  plugins: [react(), {
    name: "appearance-before-paint",
    transformIndexHtml() {
      return [{ tag: "script", injectTo: "head", children:
        `(()=>{const appearance=(${createAppearanceApi.toString()})();let storage=null;try{storage=window.localStorage;}catch{}appearance.apply(document,appearance.read(storage),new Date());})();` }];
    },
  }],
  build: { outDir: "../src/easyflow/web/static", emptyOutDir: false },
  server: { proxy: { "/api": "http://127.0.0.1:8790" } },
});
