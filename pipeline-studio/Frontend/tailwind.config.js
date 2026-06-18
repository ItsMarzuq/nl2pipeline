/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./App.jsx",
    "./main.jsx",
    "./NL2PipelineStudio.jsx", // <-- This explicitly forces Tailwind to scan your workspace file!
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}