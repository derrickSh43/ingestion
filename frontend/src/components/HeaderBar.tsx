export function HeaderBar({ name, subtitle }: { name?: string | null; subtitle?: string | null }) {
  return (
    <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
      <div>
        <div className="text-xs font-semibold text-slate-500">Welcome</div>
        <div className="text-lg font-semibold text-slate-900">{name || 'Admin'}</div>
      </div>
      <div className="text-sm text-slate-600">{subtitle || ''}</div>
    </div>
  )
}
