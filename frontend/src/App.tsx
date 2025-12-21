import { Routes, Route, Navigate } from 'react-router-dom'

import { IngestionPage } from './pages/IngestionPage'

function Placeholder({ label }: { label: string }) {
  return (
    <div style={{ padding: '2rem' }}>
      <h1>{label}</h1>
      <p>Placeholder route.</p>
    </div>
  )
}

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/ingestion" replace />} />
      <Route path="/ingestion" element={<IngestionPage />} />
      <Route path="/admin" element={<Placeholder label="Admin" />} />
      <Route path="/dashboard" element={<Placeholder label="Dashboard" />} />
      <Route path="*" element={<Placeholder label="Not Found" />} />
    </Routes>
  )
}
