import { rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const generatedAssets = fileURLToPath(new URL('../internal/http/web/static/assets', import.meta.url));

export default defineConfig({
  plugins: [
    {
      name: 'clean-generated-assets',
      apply: 'build',
      buildStart() {
        rmSync(generatedAssets, { recursive: true, force: true });
      },
    },
    react(),
  ],
  build: {
    outDir: '../internal/http/web/static',
    emptyOutDir: false,
    assetsDir: 'assets',
    sourcemap: false,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:2525',
      '/files': 'http://localhost:2525',
    },
  },
});
