"use client";

import Image from 'next/image';
import {useCallback, useRef, useState, useSyncExternalStore} from 'react';
import {ArrowDown, ArrowUpRight, Github, RotateCcw} from 'lucide-react';
import {FaceCanvas, type Mood} from './face/face-canvas';
import {faces} from './face/content';

function subscribeMotion(update: () => void) {
  const media = window.matchMedia('(prefers-reduced-motion: reduce)');
  media.addEventListener('change', update);
  return () => media.removeEventListener('change', update);
}

export function InteractiveHero() {
  const stageRef = useRef<HTMLDivElement>(null);
  const reduced = useSyncExternalStore(subscribeMotion, () => window.matchMedia('(prefers-reduced-motion: reduce)').matches, () => true);
  const [mood, setMood] = useState<Mood>({face: 'base', patience: 100, pulls: 0});
  const [reaction, setReaction] = useState(0);
  const onMood = useCallback((next: Mood) => setMood(next), []);

  return (
    <div className="seyi-hero">
      <div className="seyi-hero-intro">
        <span>Software engineer / AI solutions</span>
        <span>Lagos, Nigeria · Building globally</span>
      </div>
      <h1 className="seyi-headline">HEY, I’M <span>SEYI.</span></h1>
      <div className="seyi-hero-grid">
        <div className="seyi-bio">
          <p className="seyi-bio-lead">I build software that<br /><span className="font-serif italic text-primary">makes AI useful.</span></p>
          <p>From the idea to the infrastructure, I turn complex problems into products people can use.</p>
          <p>Building MantaJobs. Previously founding engineer at Elastic AI. Always curious about what comes next.</p>
          <a href="#systems" className="mt-3 inline-flex min-h-12 items-center gap-3 rounded-full bg-primary px-6 font-semibold text-primary-foreground">Explore my work <ArrowDown className="size-4" aria-hidden="true" /></a>
        </div>
        <div ref={stageRef} className="seyi-stage">
          {reduced && <div className="seyi-static-character"><Image src={faces.base.src} alt="A playful illustrated Seyi Ogunmusire wearing his black embroidered kaftan" width={512} height={512} priority /></div>}
          <p className={`seyi-pull-hint ${mood.pulls ? 'opacity-0' : ''}`} aria-hidden="true">{reduced ? 'A little personality.' : 'Go on, pull my face.'}<svg viewBox="0 0 70 50" fill="none"><path d="M62 5Q23 1 12 40m-4-11 4 13 13-6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg></p>
        </div>
        <aside className="seyi-hero-aside">
          <div className="seyi-mood">
            <div className="flex items-center justify-between gap-4"><span className="hero-micro">Current mood</span><span className="text-sm font-semibold" aria-live="polite">{faces[mood.face].mood}</span></div>
            <div className="mt-5 flex justify-between hero-micro"><span>Creative patience</span><span>{reduced ? '100' : mood.patience}%</span></div>
            <div role="meter" aria-label="Character patience" aria-valuemin={0} aria-valuemax={100} aria-valuenow={reduced ? 100 : mood.patience} className="mt-2 h-1.5 overflow-hidden rounded-full bg-muted"><div className="h-full origin-left rounded-full bg-primary" style={{transform: `scaleX(${reduced ? 1 : mood.patience / 100})`}} /></div>
          </div>
          <button type="button" onClick={() => setReaction(v => v + 1)} className="inline-flex min-h-11 items-center gap-2 rounded-full border border-border px-4 text-sm font-semibold" disabled={reduced}><RotateCcw className="size-4" aria-hidden="true" />{reduced ? 'Motion reduced' : 'Make me react'}</button>
          <div className="border-t border-border pt-6"><p className="hero-micro">Have something worth building?</p><a href="mailto:ogunmusireseyi01@gmail.com" className="mt-3 inline-flex min-h-11 items-center gap-2 text-lg font-semibold">Let’s talk <ArrowUpRight className="size-5" aria-hidden="true" /></a><a href="https://github.com/Theideabased" target="_blank" rel="noreferrer" className="mt-2 flex min-h-11 items-center gap-2 text-sm text-muted-foreground"><Github className="size-4" aria-hidden="true" />Explore the code <ArrowUpRight className="size-4" aria-hidden="true" /></a></div>
        </aside>
      </div>
      {!reduced && <FaceCanvas stageRef={stageRef} discColor="#BCEB27" onMood={onMood} reaction={reaction} />}
    </div>
  );
}
