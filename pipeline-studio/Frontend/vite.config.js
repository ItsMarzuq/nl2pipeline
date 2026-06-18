import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  // Force it to look right here in the Frontend folder
  root: path.resolve(__dirname),
  server: {
    port: 5173,
    cors: true
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './'),
    },
  },
});