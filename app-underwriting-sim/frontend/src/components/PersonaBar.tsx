import { Users } from "lucide-react";
import { PERSONAS, PersonaId, personaById } from "@/lib/personas";

interface PersonaBarProps {
  persona: PersonaId;
  onChange: (id: PersonaId) => void;
}

/**
 * Horizontal persona selector shown above every page. Switching persona filters
 * the sidebar to that audience's workbench (see lib/personas). Pure frontend.
 */
export default function PersonaBar({ persona, onChange }: PersonaBarProps) {
  const active = personaById(persona);

  return (
    <div className="bg-white border-b border-gray-200 px-8 py-3 flex items-center gap-4 sticky top-0 z-10">
      <div className="flex items-center gap-2 text-gray-500 flex-shrink-0">
        <Users className="w-4 h-4" />
        <span className="text-xs font-semibold uppercase tracking-wide">Viewing as</span>
      </div>
      <div className="flex items-center gap-1.5 flex-wrap">
        {PERSONAS.map((p) => (
          <button
            key={p.id}
            onClick={() => onChange(p.id)}
            className={`px-3 py-1.5 rounded-full text-xs font-medium transition-colors ${
              p.id === persona
                ? "bg-databricks-red text-white"
                : "bg-gray-100 text-gray-600 hover:bg-gray-200"
            }`}
          >
            {p.label}
          </button>
        ))}
      </div>
      <div className="ml-auto text-xs text-gray-400 hidden lg:block">{active.blurb}</div>
    </div>
  );
}
