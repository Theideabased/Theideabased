export const proofPoints = [
  { value: "#1", label: "Nosana Agent 101 Challenge" },
  { value: "4+", label: "AI systems shipped" },
  { value: "20,000+", label: "live jobs in an AI product" },
  { value: "Kaggle", label: "Datasets Expert" },
];

export const lenses = {
  architecture: {
    label: "Architecture",
    eyebrow: "From intelligence to complete products",
    title: "I design AI solutions as systems, not model demos.",
    body: "I connect interfaces, application logic, models, tools, data, state, and clear human handoffs. The result is dependable software that solves a complete problem—not simply an impressive output.",
    proof: "Full-stack AI products · autonomous coding · multi-agent workflows",
    icon: "bot",
  },
  production: {
    label: "Production",
    eyebrow: "The hard work starts after the demo",
    title: "I connect intelligence to dependable infrastructure.",
    body: "I build the APIs, authentication, storage, scheduled jobs, deployment paths, and recovery controls that turn an agent prototype into software people can rely on.",
    proof: "FastAPI · PostgreSQL · Cloud Run · Vercel · Docker · Supabase",
    icon: "workflow",
  },
  product: {
    label: "Product",
    eyebrow: "Useful intelligence over AI theatre",
    title: "Every agent needs a human reason to exist.",
    body: "I start with the repeated decision, bottleneck, or high-friction task. Then I decide where models, deterministic software, automation, and human review belong in the experience.",
    proof: "MantaJobs · 100+ active users · 20,000+ live opportunities",
    icon: "briefcase",
  },
  impact: {
    label: "Impact",
    eyebrow: "Engineering connected to adoption",
    title: "I understand what makes a system valuable—and used.",
    body: "My background in data, growth, and sales helps me translate technical capability into customer value, instrument the journey, and learn from what users actually do.",
    proof: "Kaggle Expert · Top 2% on Zindi · 250+ Copiwrite users",
    icon: "growth",
  },
} as const;

export type LensKey = keyof typeof lenses;

export const projects = [
  {
    name: "MantaJobs",
    type: "Full-stack AI product",
    summary: "A production career platform combining job aggregation, relevance matching, résumé generation, application workflows, lifecycle messaging, subscriptions, and an external jobs API.",
    outcome: "100+ active users · 20,000+ live jobs",
    tags: ["Next.js", "AI matching", "Data pipelines", "Product engineering"],
    href: "https://www.mantajobs.com",
  },
  {
    name: "Elastic AI Coding Agent",
    type: "Autonomous software engineering",
    summary: "Founding engineering work on an autonomous coding agent inside VS Code, including the conversational API connecting users to AI models and the surrounding authentication infrastructure.",
    outcome: "Production agent experience · 2025–2026",
    tags: ["Coding agents", "Model APIs", "Authentication", "VS Code"],
    href: "https://elasticapp.ai/",
  },
  {
    name: "ASI-Sopilot",
    type: "Collaborative multi-agent research",
    summary: "A coordinator routes questions to specialist agents for token analysis and market intelligence, then synthesizes their findings into one understandable response.",
    outcome: "Specialist delegation + response synthesis",
    tags: ["Multi-agent", "Fetch.ai uAgents", "Solana", "Tool use"],
    href: "https://github.com/Theideabased/ASI-Sopilot",
  },
  {
    name: "ElizaReels",
    type: "End-to-end media agent",
    summary: "A prompt-to-video agent that plans a script, generates narration, finds media, creates subtitles, and coordinates GPU rendering into a downloadable video.",
    outcome: "Built for Nosana × ElizaOS",
    tags: ["ElizaOS", "Workflow orchestration", "Video AI", "GPU compute"],
    href: "https://github.com/Theideabased/agent-challenge",
  },
  {
    name: "MastraBolt",
    type: "Agent development platform",
    summary: "A browser-based environment for creating, testing, previewing, and deploying AI agents without writing code, with multiple model providers and one-click deployment paths.",
    outcome: "1st place · Nosana Agent 101 Challenge",
    tags: ["Agent infrastructure", "Multi-model", "Browser IDE", "Deployment"],
    href: "https://nosana.com/blog/agent-101-recap-how-builders-took-on-the-nosana-challenge/",
  },
  {
    name: "Business Email Data API",
    type: "Autonomous data workflow",
    summary: "A scheduled cloud pipeline that discovers public business contacts, validates records, applies suppression rules, recovers from failures, and serves permitted data through an API.",
    outcome: "Production-ready cloud pipeline",
    tags: ["FastAPI", "PostgreSQL", "Cloud Run", "Recovery controls"],
    href: "https://github.com/Theideabased/verified_email_list",
  },
];

export const engineeringLoop = [
  { number: "01", title: "Frame the job", copy: "Define the user, desired outcome, constraints, and the decisions the system must make before choosing a model or framework." },
  { number: "02", title: "Design the control flow", copy: "Separate reasoning from deterministic steps. Define tools, state, delegation, permissions, failure paths, and human checkpoints." },
  { number: "03", title: "Connect real systems", copy: "Integrate APIs, data stores, identity, queues, scheduled workflows, and model providers into one observable product path." },
  { number: "04", title: "Test the behaviour", copy: "Exercise realistic tasks, edge cases, tool failures, and output quality. Improve the workflow from evidence instead of intuition." },
  { number: "05", title: "Ship and learn", copy: "Deploy, instrument adoption, watch where users lose trust, and turn those signals into the next engineering decision." },
];

export const capabilities = [
  { label: "Agent architecture", items: "Planning, delegation, multi-agent coordination, stateful workflows, human-in-the-loop design" },
  { label: "Models & intelligence", items: "LLM integration, recommendation systems, conversational APIs, audio AI, prompt pipelines" },
  { label: "Tools & integrations", items: "FastAPI, external APIs, authentication, payments, web automation, scheduled jobs" },
  { label: "Production systems", items: "Python, TypeScript, Next.js, PostgreSQL, Supabase, Docker, Cloud Run, Vercel" },
  { label: "Data & evaluation", items: "SQL, machine learning, analytics, experiment design, failure analysis, decision dashboards" },
  { label: "Product & adoption", items: "Problem framing, UX, positioning, lifecycle messaging, growth experiments, customer journeys" },
];

export const experience = [
  { role: "Founder & Software Engineer", place: "MantaJobs", period: "2024—now", copy: "Building an AI-assisted career platform, its application architecture, data pipelines, automation, integrations, and growth systems." },
  { role: "Founding Software Engineer", place: "Elastic AI", period: "2025—2026", copy: "Built core parts of an autonomous coding agent, conversational model APIs, and authentication infrastructure." },
  { role: "AI Agent Builder", place: "Independent projects", period: "Ongoing", copy: "Shipping multi-agent research, agent-building infrastructure, media generation workflows, and production data automation." },
  { role: "Data Analyst", place: "Ondo State payroll audit", period: "Public impact", copy: "Used evidence-led analysis and automation to help uncover more than ₦500 million in payroll fraud." },
];

export const recognition = [
  "1st Place · Nosana Agent 101 Challenge",
  "Kaggle Datasets Expert",
  "Top 2% Data Scientist on Zindi",
  "Data Science Africa competition winner",
  "AI Agents Fundamentals · Hugging Face",
  "Introduction to LangGraph · LangChain Academy",
];
