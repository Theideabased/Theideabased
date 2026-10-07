"use client";

import Image from "next/image";
import { ArrowDown, ArrowUpRight, Bot, BriefcaseBusiness, Check, ChevronRight, Github, Globe2, Linkedin, Mail, Moon, Sparkles, Sun, TrendingUp, Workflow } from "lucide-react";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useEffect, useState } from "react";
import { capabilities, engineeringLoop, experience, lenses, LensKey, projects, proofPoints, recognition } from "@/lib/portfolio-data";

/* ─────────────────────────────────────────────────────────
 * PAGE CONTENT STORYBOARD
 *    0ms   navigation and primary hero action are usable
 *  100ms   hero label settles into place
 *  250ms   portrait enters from the right
 *  420ms   proof points rise in sequence
 *  700ms   portrait annotation settles
 * ───────────────────────────────────────────────────────── */
const TIMING = { label: 100, portrait: 250, proof: 420, annotation: 700 };
const SPRING = { type: "spring" as const, stiffness: 300, damping: 30 };

const iconMap = { briefcase: BriefcaseBusiness, bot: Bot, growth: TrendingUp, workflow: Workflow };

function SectionLabel({ index, children }: { index: string; children: React.ReactNode }) {
  return (
    <div className="mb-8 flex items-center gap-3 font-mono text-xs font-semibold uppercase tracking-[0.16em] text-muted-foreground">
      <span className="text-primary">{index}</span><span className="h-px w-8 bg-border" aria-hidden="true" /><span>{children}</span>
    </div>
  );
}

function ExternalArrow() {
  return <ArrowUpRight aria-hidden="true" className="relative -top-px size-4" strokeWidth={1.8} />;
}

export function Portfolio() {
  const reduceMotion = useReducedMotion();
  const [stage, setStage] = useState(0);
  const [lens, setLens] = useState<LensKey>("architecture");
  const [dark, setDark] = useState(true);

  useEffect(() => {
    if (reduceMotion) return;
    const timers = [
      window.setTimeout(() => setStage(1), TIMING.label),
      window.setTimeout(() => setStage(2), TIMING.portrait),
      window.setTimeout(() => setStage(3), TIMING.proof),
      window.setTimeout(() => setStage(4), TIMING.annotation),
    ];
    return () => timers.forEach(window.clearTimeout);
  }, [reduceMotion]);

  function toggleTheme() {
    const next = !dark;
    setDark(next);
    document.documentElement.classList.toggle("dark", next);
  }

  const activeLens = lenses[lens];
  const ActiveLensIcon = iconMap[activeLens.icon];
  const visibleStage = reduceMotion ? 4 : stage;

  return (
    <div className="min-h-screen overflow-x-hidden bg-background text-foreground">
      <a href="#main-content" className="fixed left-4 top-4 z-[100] -translate-y-24 rounded-md bg-primary px-4 py-3 text-sm font-semibold text-primary-foreground focus:translate-y-0">Skip to content</a>
      <header className="fixed inset-x-0 top-0 z-50 border-b border-border/70 bg-background/82 backdrop-blur-xl">
        <nav className="mx-auto flex h-18 max-w-7xl items-center justify-between px-5 md:px-8" aria-label="Main navigation">
          <a href="#top" className="flex min-h-11 items-center gap-3 rounded-md pr-2 font-semibold tracking-tight">
            <span className="grid size-8 place-items-center rounded-full bg-primary font-mono text-xs font-bold text-primary-foreground">SO</span>
            <span className="hidden sm:inline">Seyi Ogunmusire</span>
          </a>
          <div className="flex items-center gap-1 sm:gap-2">
            <a href="#work" className="hidden min-h-11 items-center rounded-md px-3 text-sm text-muted-foreground transition-colors duration-100 hover:text-foreground md:flex">Work</a>
            <a href="#architecture" className="hidden min-h-11 items-center rounded-md px-3 text-sm text-muted-foreground transition-colors duration-100 hover:text-foreground md:flex">Architecture</a>
            <button type="button" onClick={toggleTheme} className="grid size-11 place-items-center rounded-full text-muted-foreground transition-colors duration-100 hover:bg-muted hover:text-foreground" aria-label={dark ? "Switch to light theme" : "Switch to dark theme"}>
              {dark ? <Sun className="size-4" aria-hidden="true" /> : <Moon className="size-4" aria-hidden="true" />}
            </button>
            <a href="mailto:ogunmusireseyi01@gmail.com" className="inline-flex min-h-11 items-center gap-2 rounded-full bg-primary px-4 text-sm font-semibold text-primary-foreground transition-transform duration-150 motion-safe:hover:scale-[1.03] active:scale-[0.97]">Let&apos;s talk <ArrowUpRight className="size-4" aria-hidden="true" /></a>
          </div>
        </nav>
      </header>

      <main id="main-content">
        <section id="top" className="relative isolate overflow-hidden bg-brand-bg pt-28 md:pt-36">
          <div className="noise-field pointer-events-none absolute inset-0 -z-10" aria-hidden="true" />
          <div className="mx-auto grid min-h-[calc(100svh-7rem)] max-w-7xl items-center gap-16 px-5 pb-20 md:px-8 lg:grid-cols-[1.15fr_0.85fr] lg:gap-20 lg:pb-24">
            <div className="max-w-4xl">
              <motion.p initial={reduceMotion ? false : { opacity: 0, y: -10 }} animate={{ opacity: visibleStage >= 1 ? 1 : 0, y: visibleStage >= 1 ? 0 : -10 }} transition={SPRING} className="mb-7 flex items-center gap-3 font-mono text-xs font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                <span className="size-2 rounded-full bg-primary" aria-hidden="true" /> Senior agentic AI engineer · product builder
              </motion.p>
              <h1 className="text-balance font-serif text-[clamp(3.5rem,8.5vw,8.4rem)] leading-[0.84] tracking-[-0.065em]">I engineer AI agents that move from reasoning<span className="mt-2 block text-primary">to reliable action.</span></h1>
              <p className="text-pretty mt-9 max-w-2xl text-lg leading-8 text-muted-foreground md:text-xl">I design and ship agentic systems that plan, use tools, coordinate specialist agents, connect to real infrastructure, and produce outcomes people can trust.</p>
              <div className="mt-10 flex flex-wrap gap-3">
                <a href="#work" className="inline-flex min-h-12 items-center gap-2 rounded-full bg-primary px-6 font-semibold text-primary-foreground transition-transform duration-150 motion-safe:hover:scale-[1.03] active:scale-[0.97]">Explore agent systems <ArrowDown className="size-4" aria-hidden="true" /></a>
                <a href="https://github.com/Theideabased" target="_blank" rel="noreferrer" className="inline-flex min-h-12 items-center gap-2 rounded-full border border-border bg-background/50 px-6 font-semibold transition-colors duration-100 hover:bg-muted">View GitHub <ExternalArrow /></a>
              </div>
            </div>
            <motion.div initial={reduceMotion ? false : { opacity: 0, x: 24 }} animate={{ opacity: visibleStage >= 2 ? 1 : 0, x: visibleStage >= 2 ? 0 : 24 }} transition={SPRING} className="relative mx-auto w-full max-w-md lg:max-w-none">
              <div className="relative aspect-[4/5] overflow-hidden rounded-[1.5rem] border border-border bg-card">
                <Image src="/images/seyi-ogunmusire-clear.webp" alt="Seyi Ogunmusire wearing traditional Nigerian attire" fill priority sizes="(max-width: 1024px) 448px, 38vw" className="portrait-image object-cover object-[center_18%]" />
                <div className="absolute inset-x-0 bottom-0 h-1/2 bg-gradient-to-t from-background via-background/20 to-transparent" aria-hidden="true" />
                <motion.div initial={reduceMotion ? false : { opacity: 0, y: 12 }} animate={{ opacity: visibleStage >= 4 ? 1 : 0, y: visibleStage >= 4 ? 0 : 12 }} transition={SPRING} className="absolute inset-x-5 bottom-5 rounded-xl border border-border bg-background/88 p-4 backdrop-blur-lg">
                  <p className="font-mono text-[0.68rem] font-semibold uppercase tracking-[0.14em] text-primary">Engineering thesis</p>
                  <p className="mt-2 text-sm leading-6">A capable agent reasons. A production agent also knows when to act, retry, escalate, and stop.</p>
                </motion.div>
              </div>
              <div className="absolute -right-3 -top-3 grid size-16 place-items-center rounded-full border border-border bg-primary font-mono text-xs font-bold text-primary-foreground md:-right-6 md:-top-6" aria-hidden="true">LAG<br />OS</div>
            </motion.div>
          </div>
          <div className="border-y border-border bg-background/65 backdrop-blur-sm">
            <div className="mx-auto grid max-w-7xl grid-cols-2 px-5 md:px-8 lg:grid-cols-4">
              {proofPoints.map((item, index) => (
                <motion.div key={item.label} initial={reduceMotion ? false : { opacity: 0, y: 20 }} animate={{ opacity: visibleStage >= 3 ? 1 : 0, y: visibleStage >= 3 ? 0 : 20 }} transition={{ ...SPRING, delay: reduceMotion ? 0 : index * 0.12 }} className="border-border px-4 py-7 first:pl-0 even:border-l even:pl-5 lg:border-l lg:px-7 lg:first:border-l-0 lg:first:px-0">
                  <p className="font-mono text-2xl font-bold tracking-tight text-primary md:text-3xl">{item.value}</p><p className="mt-2 max-w-44 text-sm leading-5 text-muted-foreground">{item.label}</p>
                </motion.div>
              ))}
            </div>
          </div>
        </section>

        <section id="work" className="scroll-mt-24 px-5 py-24 md:px-8 md:py-32">
          <div className="mx-auto max-w-7xl">
            <SectionLabel index="01">Agentic engineering</SectionLabel>
            <div className="grid gap-12 lg:grid-cols-[0.88fr_1.12fr] lg:items-center">
              <div>
                <p className="font-mono text-xs font-semibold uppercase tracking-[0.14em] text-primary">System thinking beyond the prompt</p>
                <h2 className="text-balance mt-5 font-serif text-5xl leading-[0.95] tracking-[-0.045em] md:text-7xl">The model is one component. The workflow is the product.</h2>
                <p className="mt-7 max-w-xl text-lg leading-8 text-muted-foreground">I work across the full agent lifecycle: problem framing, orchestration, tool interfaces, state, backend services, deployment, failure handling, and the product experience surrounding the intelligence.</p>
                <a href="#systems" className="mt-8 inline-flex min-h-11 items-center gap-2 rounded-md font-semibold text-primary underline decoration-primary/35 underline-offset-8 transition-colors duration-100 hover:text-foreground">See shipped systems <ArrowDown className="size-4" aria-hidden="true" /></a>
              </div>
              <div className="agent-map rounded-2xl border border-border bg-card p-5 md:p-8" aria-label="Diagram of an agentic system moving from user intent through orchestration and tools to a verified outcome">
                <div className="flex items-center justify-between border-b border-border pb-5">
                  <div><p className="font-mono text-xs font-semibold uppercase tracking-[0.14em] text-primary">Agent runtime</p><p className="mt-1 text-sm text-muted-foreground">A production control loop</p></div>
                  <span className="flex items-center gap-2 font-mono text-xs text-muted-foreground"><span className="size-2 rounded-full bg-primary" aria-hidden="true" /> observable</span>
                </div>
                <div className="mt-6 grid gap-3 sm:grid-cols-2">
                  {["01 · Intent + context", "02 · Plan + route", "03 · Tools + agents", "04 · Validate + respond"].map((step, index) => (
                    <div key={step} className={`rounded-xl border p-4 ${index === 1 || index === 2 ? "border-primary/50 bg-primary/5" : "border-border bg-background"}`}>
                      <p className="font-mono text-xs font-semibold text-primary">{step}</p>
                      <p className="mt-3 text-sm leading-6 text-muted-foreground">{["Understand the goal, constraints, memory, and permission boundary.", "Choose the next action, delegate work, and persist state.", "Call APIs, retrieve data, run specialist agents, and handle failures.", "Check the result, request human input when needed, and close the loop."][index]}</p>
                    </div>
                  ))}
                </div>
                <div className="mt-3 flex flex-wrap gap-2 rounded-xl border border-border bg-background p-4 font-mono text-[0.72rem] text-muted-foreground">
                  {["state", "tracing", "guardrails", "retries", "human review", "evaluation"].map((item) => <span key={item} className="rounded-full border border-border px-3 py-1.5">{item}</span>)}
                </div>
              </div>
            </div>
            <div className="mt-12 grid gap-px overflow-hidden rounded-2xl border border-border bg-border md:grid-cols-3">
              {[
                { title: "Agent architecture", text: "Planning, specialist delegation, tool selection, state management, and clear control boundaries." },
                { title: "Production engineering", text: "APIs, identity, databases, cloud deployment, scheduled workflows, recovery paths, and integrations." },
                { title: "Product judgment", text: "Human checkpoints, understandable outputs, measurable adoption, and business value beyond the demo." },
              ].map((item) => <article key={item.title} className="bg-card p-7 md:p-8"><h3 className="font-semibold">{item.title}</h3><p className="mt-3 text-sm leading-6 text-muted-foreground">{item.text}</p></article>)}
            </div>
          </div>
        </section>

        <section id="architecture" className="scroll-mt-24 border-y border-border bg-card/45 px-5 py-24 md:px-8 md:py-32">
          <div className="mx-auto max-w-7xl">
            <SectionLabel index="02">Senior engineering lens</SectionLabel>
            <div className="grid gap-12 lg:grid-cols-[0.7fr_1.3fr] lg:gap-20">
              <div>
                <h2 className="text-balance font-serif text-5xl leading-[0.95] tracking-[-0.045em] md:text-6xl">I think across the whole system.</h2>
                <p className="mt-6 max-w-md leading-7 text-muted-foreground">An agent succeeds when architecture, infrastructure, product judgment, and adoption reinforce one another.</p>
                <div className="mt-9 grid grid-cols-2 gap-2" aria-label="Choose a perspective">
                  {(Object.keys(lenses) as LensKey[]).map((key) => {
                    const item = lenses[key]; const Icon = iconMap[item.icon]; const selected = lens === key;
                    return (
                      <button key={key} type="button" aria-pressed={selected} onClick={() => setLens(key)} className={`flex min-h-12 items-center gap-3 rounded-lg border px-4 text-left text-sm font-semibold transition-colors duration-100 ${selected ? "border-primary bg-primary text-primary-foreground" : "border-border bg-background text-muted-foreground hover:bg-muted hover:text-foreground"}`}>
                        <Icon className="size-4" aria-hidden="true" />{item.label}
                      </button>
                    );
                  })}
                </div>
              </div>
              <div className="min-h-[28rem] rounded-2xl border border-border bg-background p-7 md:p-10">
                <AnimatePresence mode="wait" initial={false}>
                  <motion.div key={lens} initial={reduceMotion ? false : { opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={reduceMotion ? undefined : { opacity: 0, y: -8 }} transition={{ duration: reduceMotion ? 0 : 0.2, ease: [0.16, 1, 0.3, 1] }} className="flex h-full flex-col">
                    <div className="grid size-12 place-items-center rounded-full bg-primary text-primary-foreground"><ActiveLensIcon className="size-5" aria-hidden="true" /></div>
                    <p className="mt-10 font-mono text-xs font-semibold uppercase tracking-[0.14em] text-primary">{activeLens.eyebrow}</p>
                    <h3 className="text-balance mt-4 max-w-3xl font-serif text-4xl leading-[1.02] tracking-[-0.035em] md:text-5xl">{activeLens.title}</h3>
                    <p className="mt-6 max-w-2xl text-lg leading-8 text-muted-foreground">{activeLens.body}</p>
                    <p className="mt-auto pt-10 font-mono text-sm font-semibold text-foreground">{activeLens.proof}</p>
                  </motion.div>
                </AnimatePresence>
              </div>
            </div>
          </div>
        </section>

        <section id="systems" className="scroll-mt-24 px-5 py-24 md:px-8 md:py-32">
          <div className="mx-auto max-w-7xl">
            <SectionLabel index="03">Selected systems</SectionLabel>
            <div className="mb-12 grid gap-6 md:grid-cols-[1fr_0.65fr] md:items-end">
              <h2 className="text-balance font-serif text-5xl leading-[0.95] tracking-[-0.045em] md:text-7xl">Agents that do more than talk.</h2>
              <p className="text-lg leading-8 text-muted-foreground">Agent infrastructure, autonomous coding, collaborative research, media generation, and production automation—built around work that must actually get done.</p>
            </div>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {projects.map((project, index) => (
                <motion.a key={project.name} href={project.href} target="_blank" rel="noreferrer" initial={reduceMotion ? false : { opacity: 0, y: 20 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true, amount: 0.15 }} transition={{ ...SPRING, delay: reduceMotion ? 0 : (index % 3) * 0.06 }} className={`project-surface group flex min-h-80 flex-col rounded-2xl border border-border bg-card p-7 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-4 focus-visible:ring-offset-background ${index === 0 ? "md:col-span-2 lg:col-span-2" : ""}`}>
                  <div className="flex items-start justify-between gap-5"><p className="font-mono text-[0.68rem] font-semibold uppercase tracking-[0.13em] text-primary">{project.type}</p><ArrowUpRight className="size-5 shrink-0 text-muted-foreground transition-transform duration-150 motion-safe:group-hover:-translate-y-1 motion-safe:group-hover:translate-x-1" aria-hidden="true" /></div>
                  <h3 className={`mt-8 font-serif leading-none tracking-[-0.035em] ${index === 0 ? "text-5xl md:text-6xl" : "text-4xl"}`}>{project.name}</h3>
                  <p className="mt-5 max-w-2xl leading-7 text-muted-foreground">{project.summary}</p>
                  <div className="mt-auto pt-8"><p className="mb-5 font-mono text-sm font-semibold text-foreground">{project.outcome}</p><div className="flex flex-wrap gap-2">{project.tags.map((tag) => <span key={tag} className="rounded-full border border-border px-3 py-1.5 text-xs text-muted-foreground">{tag}</span>)}</div></div>
                </motion.a>
              ))}
            </div>
          </div>
        </section>

        <section className="border-y border-border bg-card/45 px-5 py-24 md:px-8 md:py-32">
          <div className="mx-auto max-w-7xl">
            <SectionLabel index="04">How I build</SectionLabel>
            <div className="grid gap-8 lg:grid-cols-[0.65fr_1.35fr] lg:gap-20">
              <div>
                <h2 className="text-balance font-serif text-5xl leading-[0.95] tracking-[-0.045em] md:text-6xl">A disciplined loop from ambiguity to production.</h2>
                <p className="mt-6 max-w-md leading-7 text-muted-foreground">Senior agent engineering is equal parts architecture, software judgment, and curiosity about failure.</p>
              </div>
              <ol className="border-t border-border">
                {engineeringLoop.map((item) => (
                  <li key={item.number} className="grid gap-3 border-b border-border py-6 sm:grid-cols-[3rem_0.5fr_1fr] sm:gap-6">
                    <span className="font-mono text-xs font-semibold text-primary">{item.number}</span>
                    <h3 className="font-semibold">{item.title}</h3>
                    <p className="text-sm leading-6 text-muted-foreground">{item.copy}</p>
                  </li>
                ))}
              </ol>
            </div>

            <div className="mt-24">
              <div className="mb-10 grid gap-5 md:grid-cols-[1fr_0.7fr] md:items-end">
                <h2 className="font-serif text-5xl leading-[0.95] tracking-[-0.045em] md:text-6xl">The range to own the system.</h2>
                <p className="leading-7 text-muted-foreground">Deep enough for the architecture. Broad enough to connect it to infrastructure, users, and measurable value.</p>
              </div>
              <div className="grid gap-px overflow-hidden rounded-2xl border border-border bg-border md:grid-cols-2 lg:grid-cols-3">
                {capabilities.map((capability) => (
                  <article key={capability.label} className="bg-background p-7">
                    <h3 className="font-mono text-xs font-semibold uppercase tracking-[0.13em] text-primary">{capability.label}</h3>
                    <p className="mt-4 text-sm leading-6 text-muted-foreground">{capability.items}</p>
                  </article>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section className="px-5 py-24 md:px-8 md:py-32">
          <div className="mx-auto grid max-w-7xl gap-16 lg:grid-cols-2">
            <div>
              <SectionLabel index="05">Experience</SectionLabel>
              <h2 className="font-serif text-5xl leading-[0.95] tracking-[-0.045em] md:text-6xl">Engineering shaped by shipped work.</h2>
              <div className="mt-12">
                {experience.map((item) => (
                  <article key={`${item.role}-${item.place}`} className="grid gap-3 border-t border-border py-7 sm:grid-cols-[1fr_auto]">
                    <div><h3 className="font-semibold">{item.role} · {item.place}</h3><p className="mt-2 max-w-xl text-sm leading-6 text-muted-foreground">{item.copy}</p></div>
                    <p className="font-mono text-xs text-muted-foreground sm:text-right">{item.period}</p>
                  </article>
                ))}
              </div>
            </div>
            <div>
              <SectionLabel index="06">Signals</SectionLabel>
              <div className="rounded-2xl border border-border bg-background p-7 md:p-9">
                <p className="font-mono text-xs font-semibold uppercase tracking-[0.14em] text-primary">Proof beyond the portfolio</p>
                <ul className="mt-7">
                  {recognition.map((item) => <li key={item} className="flex items-start gap-4 border-t border-border py-5 first:border-t-0 first:pt-0"><span className="mt-0.5 grid size-6 shrink-0 place-items-center rounded-full bg-primary text-primary-foreground"><Check className="size-3.5" aria-hidden="true" /></span><span className="leading-6">{item}</span></li>)}
                </ul>
                <div className="mt-8 flex flex-wrap gap-3">
                  <a href="https://www.kaggle.com/ogunmusireseyi/" target="_blank" rel="noreferrer" className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-sm font-semibold transition-colors duration-100 hover:bg-muted">Kaggle profile <ExternalArrow /></a>
                  <a href="https://zindi.africa/users/Man_of_God" target="_blank" rel="noreferrer" className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-sm font-semibold transition-colors duration-100 hover:bg-muted">Zindi profile <ExternalArrow /></a>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section className="px-5 py-24 md:px-8 md:py-32">
          <div className="mx-auto max-w-7xl overflow-hidden rounded-3xl border border-border bg-brand-accent p-8 text-primary-foreground md:p-14 lg:p-18">
            <div className="grid gap-14 lg:grid-cols-[1.15fr_0.85fr] lg:items-end">
              <div><div className="mb-8 flex items-center gap-3 font-mono text-xs font-bold uppercase tracking-[0.15em]"><Sparkles className="size-4" aria-hidden="true" /> Building a serious agentic system?</div><h2 className="text-balance font-serif text-5xl leading-[0.92] tracking-[-0.05em] md:text-7xl">Bring me the problem that resists a simple prompt.</h2></div>
              <div><p className="text-lg leading-8 opacity-80">I am interested in senior agentic engineering work where architecture, tool use, reliability, product judgment, and real-world impact all matter.</p><a href="mailto:ogunmusireseyi01@gmail.com?subject=Senior%20Agentic%20AI%20Engineering%20Opportunity" className="mt-8 inline-flex min-h-12 items-center gap-2 rounded-full bg-primary-foreground px-6 font-semibold text-foreground transition-transform duration-150 focus-visible:outline-primary-foreground motion-safe:hover:scale-[1.03] active:scale-[0.97]">Discuss a role <ChevronRight className="size-4" aria-hidden="true" /></a></div>
            </div>
          </div>
        </section>
      </main>
      <footer className="border-t border-border px-5 py-10 md:px-8">
        <div className="mx-auto flex max-w-7xl flex-col gap-7 sm:flex-row sm:items-center sm:justify-between">
          <div><p className="font-semibold">Seyi Ogunmusire</p><p className="mt-1 text-sm text-muted-foreground">Built from Lagos—with curiosity, evidence, and care.</p></div>
          <div className="flex items-center gap-2">
            {[
              { href: "https://github.com/Theideabased", label: "GitHub", icon: Github },
              { href: "https://www.linkedin.com/in/ogunmusire-seyi", label: "LinkedIn", icon: Linkedin },
              { href: "https://www.kaggle.com/ogunmusireseyi/", label: "Kaggle", icon: Globe2 },
              { href: "mailto:ogunmusireseyi01@gmail.com", label: "Email", icon: Mail },
            ].map((item) => {
              const Icon = item.icon;
              return <a key={item.label} href={item.href} target={item.href.startsWith("http") ? "_blank" : undefined} rel={item.href.startsWith("http") ? "noreferrer" : undefined} aria-label={item.label} className="grid size-11 place-items-center rounded-full border border-border text-muted-foreground transition-colors duration-100 hover:bg-muted hover:text-foreground"><Icon className="size-4" aria-hidden="true" /></a>;
            })}
          </div>
        </div>
      </footer>
    </div>
  );
}
