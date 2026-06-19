import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  // Sets the workspace root to the main folder so Tailwind can find its config files
  root: path.resolve(__dirname), 
  server: {
    port: 5173,
    cors: true
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './Frontend'),
    },
  },
});