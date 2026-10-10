import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";

const TYPES = [
  { id: "temperature", label: "Température", unit: "°C", color: "#E0703A" },
  { id: "humidite", label: "Humidité", unit: "%", color: "#3B82C4" },
  { id: "vibration", label: "Vibration", unit: "Hz", color: "#18C79A" },
];
const MAX_POINTS = 60;
const iso = (s) => (/(Z|[+-]\d\d:?\d\d)$/.test(s) ? s : s + "Z"); // l'API renvoie de l'UTC

function mergePoints(old = [], extra = []) {
  const m = new Map(old.map((p) => [p.t, p]));
  extra.forEach((p) => m.set(p.t, p));
  return [...m.values()].sort((a, b) => a.t - b.t).slice(-MAX_POINTS);
}

function LineChart({ points, color, unit }) {
  const W = 520, H = 170, P = { l: 44, r: 10, t: 10, b: 22 };
  if (points.length < 2) return <div className="empty">En attente de données…</div>;
  const lo = Math.min(...points.map((p) => p.minimum)), hi = Math.max(...points.map((p) => p.maximum));
  const pad = (hi - lo) * 0.1 || 1, y0 = lo - pad, y1 = hi + pad;
  const t0 = points[0].t, t1 = points[points.length - 1].t || t0 + 1;
  const x = (t) => P.l + ((t - t0) / (t1 - t0 || 1)) * (W - P.l - P.r);
  const y = (v) => P.t + (1 - (v - y0) / (y1 - y0)) * (H - P.t - P.b);
  const line = points.map((p, i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)} ${y(p.moyenne).toFixed(1)}`).join("");
  const band = points.map((p) => `${x(p.t).toFixed(1)} ${y(p.maximum).toFixed(1)}`).join("L") + "L" +
    [...points].reverse().map((p) => `${x(p.t).toFixed(1)} ${y(p.minimum).toFixed(1)}`).join("L");
  const ticks = [y0 + pad, (y0 + y1) / 2, y1 - pad];
  const hhmmss = (t) => new Date(t).toLocaleTimeString("fr-FR");
  return (
    <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`Courbe de la moyenne ${unit}`}>
      {ticks.map((v, i) => (
        <g key={i}>
          <line x1={P.l} x2={W - P.r} y1={y(v)} y2={y(v)} className="grid" />
          <text x={P.l - 6} y={y(v) + 4} textAnchor="end" className="tick">{v.toFixed(1)}</text>
        </g>
      ))}
      <path d={`M${band}Z`} fill={color} opacity="0.15" />
      <path d={line} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" />
      <text x={P.l} y={H - 4} className="tick">{hhmmss(t0)}</text>
      <text x={W - P.r} y={H - 4} textAnchor="end" className="tick">{hhmmss(t1)}</text>
    </svg>
  );
}

function App() {
  const [snap, setSnap] = useState(null);
  const [live, setLive] = useState(false);
  const [capteur, setCapteur] = useState("capteur-1");
  const [hist, setHist] = useState({}); // "capteur|type" -> points

  const toPoint = (a) => ({ t: new Date(iso(a.fenetre_debut)).getTime(), moyenne: a.moyenne, minimum: a.minimum, maximum: a.maximum });

  // Flux temps réel : Server-Sent Events (reconnexion automatique par le navigateur)
  useEffect(() => {
    const es = new EventSource("/api/stream");
    es.onopen = () => setLive(true);
    es.onerror = () => setLive(false);
    es.onmessage = (e) => {
      const d = JSON.parse(e.data);
      if (!d.agregats) return;
      setSnap(d);
      setHist((h) => {
        const n = { ...h };
        d.agregats.forEach((a) => { const k = `${a.capteur_id}|${a.type}`; n[k] = mergePoints(n[k], [toPoint(a)]); });
        return n;
      });
    };
    return () => es.close();
  }, []);

  // Pré-remplissage de l'historique du capteur choisi (API REST paginée)
  useEffect(() => {
    TYPES.forEach(async (t) => {
      try {
        const r = await fetch(`/api/capteurs/${capteur}/agregats?type=${t.id}&taille=${MAX_POINTS}`);
        const j = await r.json();
        setHist((h) => ({ ...h, [`${capteur}|${t.id}`]: mergePoints(h[`${capteur}|${t.id}`], j.items.map(toPoint)) }));
      } catch (_) { /* le flux SSE alimentera quand même la courbe */ }
    });
  }, [capteur]);

  const capteurs = useMemo(() => [...new Set((snap?.agregats || []).map((a) => a.capteur_id))]
    .sort((a, b) => a.length - b.length || a.localeCompare(b)), [snap]);
  const last = (type) => (snap?.agregats || []).find((a) => a.capteur_id === capteur && a.type === type);
  const alertes = snap?.alertes || [];
  const nbActives = snap?.nb_alertes_actives ?? alertes.length;   // total réel, la liste est limitée à 20

  return (
    <main>
      <header>
        <div>
          <p className="kicker">HETIC · Mastère Data & IA</p>
          <h1>Supervision IoT en temps réel</h1>
        </div>
        <div className={`status ${live ? "on" : "off"}`}>
          <span className="dot" /> {live ? "Flux SSE connecté" : "Reconnexion…"}
          {snap && <small>maj {new Date(snap.ts).toLocaleTimeString("fr-FR")}</small>}
        </div>
      </header>

      <section className="kpis">
        <div className="kpi"><b>{capteurs.length || "–"}</b><span>capteurs actifs</span></div>
        <div className="kpi"><b>{snap ? snap.agregats.length : "–"}</b><span>courbes suivies</span></div>
        <div className={`kpi ${nbActives ? "warn" : ""}`}><b>{nbActives}</b><span>alertes actives</span></div>
      </section>

      <nav aria-label="Choix du capteur">
        {(capteurs.length ? capteurs : [capteur]).map((c) => (
          <button key={c} className={c === capteur ? "sel" : ""} onClick={() => setCapteur(c)}>{c.replace("capteur-", "C")}</button>
        ))}
      </nav>

      <section className="charts">
        {TYPES.map((t) => {
          const a = last(t.id);
          return (
            <article key={t.id}>
              <h2>{t.label} <span>· {capteur}</span></h2>
              <p className="big" style={{ color: t.color }}>
                {a ? a.moyenne.toFixed(2) : "–"}<small> {t.unit} (moyenne 30 s)</small>
              </p>
              {a && <p className="mm">min {a.minimum.toFixed(2)} · max {a.maximum.toFixed(2)} · σ {a.ecart_type.toFixed(2)} · n={a.nb_mesures}</p>}
              <LineChart points={hist[`${capteur}|${t.id}`] || []} color={t.color} unit={t.unit} />
            </article>
          );
        })}
      </section>

      <section className="alerts">
        <h2>Alertes actives <span>(les {alertes.length} plus récentes sur {nbActives})</span></h2>
        {alertes.length === 0 ? <p className="empty">Aucune alerte active.</p> : (
          <table>
            <thead><tr><th>Capteur</th><th>Type</th><th>Valeur</th><th>Heure</th></tr></thead>
            <tbody>
              {alertes.map((a, i) => (
                <tr key={i}><td>{a.capteur_id}</td><td>{a.type}</td><td>{Number(a.valeur).toFixed(2)}</td>
                  <td>{new Date(a.event_time).toLocaleTimeString("fr-FR")}</td></tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<App />);
