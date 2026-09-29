export const proofPoints = [
  { value: "20,000+", label: "live jobs on MantaJobs" },
  { value: "250+", label: "users grown at Copiwrite" },
  { value: "₦500M", label: "payroll fraud uncovered" },
  { value: "#1", label: "Nosana Agent 101 Challenge" },
];

export const lenses = {
  product: {
    label: "Product",
    eyebrow: "From friction to a product people use",
    title: "I start with the person, not the feature list.",
    body: "MantaJobs began with a familiar problem: talented people were spending too much time searching and too little time progressing. I turned that friction into one connected journey—from discovery to application tracking.",
    proof: "100+ active users · 20,000+ live opportunities",
    icon: "briefcase",
  },
  ai: {
    label: "AI systems",
    eyebrow: "Useful intelligence, not theatre",
    title: "The agent should make the next decision easier.",
    body: "I build agents that research markets, create video, match people with jobs, and help developers ship. The technology stays behind the outcome, where it belongs.",
    proof: "1st place · Nosana Agent 101 Challenge",
    icon: "bot",
  },
  growth: {
    label: "Sales & growth",
    eyebrow: "A good product still needs momentum",
    title: "I build the message, the journey, and the follow-up.",
    body: "From paid campaigns and cold outreach to lifecycle email and landing-page analysis, I connect what a product does with the reason someone should care—and measure what happens next.",
    proof: "250+ Copiwrite users · campaigns, outreach, conversion",
    icon: "growth",
  },
  data: {
    label: "Data",
    eyebrow: "Evidence before opinion",
    title: "I look for the pattern hiding beneath the noise.",
    body: "My data work spans public-sector fraud detection, production pipelines, machine learning, and decision dashboards. The goal is always the same: turn information into an action someone can take.",
    proof: "Kaggle Expert · Top 2% on Zindi · ₦500M identified",
    icon: "data",
  },
} as const;

export type LensKey = keyof typeof lenses;

export const projects = [
  {
    name: "MastraBolt",
    type: "Award-winning AI platform",
    summary: "A browser workspace where people can create, test, and deploy AI agents without writing code.",
    outcome: "1st place · Nosana Agent 101 Challenge",
    tags: ["AI agents", "No-code", "Deployment"],
    href: "https://nosana.com/blog/agent-101-recap-how-builders-took-on-the-nosana-challenge/",
  },
  {
    name: "Copiwrite",
    type: "Marketing and growth product",
    summary: "A practical growth system helping Nigerian businesses clarify offers, create stronger campaigns, and follow up with buyers.",
    outcome: "Grown to 250+ users",
    tags: ["Positioning", "Campaigns", "Sales systems"],
    href: "https://www.copiwrite.com",
  },
  {
    name: "ASI-Sopilot",
    type: "Collaborative AI agents",
    summary: "A conversational research system where specialist agents investigate Solana tokens and markets, then combine their findings.",
    outcome: "Research made understandable",
    tags: ["Multi-agent", "Solana", "Market research"],
    href: "https://github.com/Theideabased/ASI-Sopilot",
  },
  {
    name: "ElizaReels",
    type: "Prompt-to-video agent",
    summary: "One prompt becomes a script, voiceover, media sequence, subtitles, and a rendered short-form video.",
    outcome: "Built for Nosana × ElizaOS",
    tags: ["ElizaOS", "Video AI", "GPU orchestration"],
    href: "https://github.com/Theideabased/agent-challenge",
  },
  {
    name: "Business Email Data API",
    type: "Responsible data automation",
    summary: "A scheduled cloud pipeline that discovers public business contacts, validates records, applies suppression rules, and serves permitted data.",
    outcome: "Production-ready cloud pipeline",
    tags: ["FastAPI", "PostgreSQL", "Cloud Run"],
    href: "https://github.com/Theideabased/verified_email_list",
  },
  {
    name: "Yoruba Talking Drum AI",
    type: "Cultural machine learning",
    summary: "Software that classifies talking-drum audio into tonic-solfa notes, making a cultural research project accessible on the web.",
    outcome: "Machine learning for cultural preservation",
    tags: ["Audio AI", "PyTorch", "Research"],
    href: "https://github.com/Theideabased/yomi_talking_drum",
  },
];

export const experience = [
  { role: "Founder & Product Lead", place: "MantaJobs", period: "2024—now", copy: "Building an AI-assisted career platform and the operating systems behind it." },
  { role: "Founding Software Engineer", place: "Elastic AI", period: "2025—2026", copy: "Built an autonomous coding agent, conversational model APIs, and authentication infrastructure." },
  { role: "Data Analyst", place: "Ondo State payroll audit", period: "Public impact", copy: "Helped uncover more than ₦500 million in payroll fraud through evidence-led analysis." },
  { role: "AI & Robotics Instructor", place: "University of Lagos", period: "2 years", copy: "Taught Python, robotics, and practical problem-solving to younger builders." },
];

export const recognition = [
  "Kaggle Datasets Expert",
  "Top 2% Data Scientist on Zindi",
  "Data Science Africa competition winner",
  "AI & Robotics instructor at UNILAG",
  "BSc Actuarial Science · GPA 4.42/5.00",
];
