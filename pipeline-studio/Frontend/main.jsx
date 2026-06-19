import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'  // <-- MAKE SURE THIS LINE IS EXACTLY HERE! 🎉
import App from './App';

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)