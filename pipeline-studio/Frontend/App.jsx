import React from 'react';
import NL2PipelineStudio from './NL2PipelineStudio'; // Clean relative path mapping

function App() {
  return (
    <div className="min-h-screen bg-slate-950 text-slate-50 antialiased">
      <NL2PipelineStudio />
    </div>
  );
}

// Crucial: Vite requires this explicit default export to mount in main.jsx!
export default App;