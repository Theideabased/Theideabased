# Seyi character artwork

Generated with the built-in image generation tool from the portfolio owner's existing portrait at `public/images/seyi-ogunmusire-clear.webp`.

Final prompt: Create a transparent 4 × 4 sheet of consistently framed, premium 3D cartoon head-and-shoulders portraits resembling Seyi: Nigerian Black man, deep brown skin, short textured fade, trimmed beard and mustache, wearing his black embroidered kaftan. Preserve facial identity, camera, lighting, scale, and placement across expressions. Expressions in row order: neutral, look left, look right, cross-eyed; tongue out, startled, laughing, skeptical; whistling, smug, dizzy, grimacing; ouch, pinched cheeks, happy, neutral. No text, grid lines, or background.

`expression-sheet.png` preserves the original artwork. `scripts/prepare-seyi-faces.mjs` extracts and pads each cell into a transparent 512 × 512 WebP texture. The original generated sheet is 1254 × 1254 pixels, so rounded cell boundaries are used. The padding and `components/face/content.ts` landmarks register the artwork to the face renderer.

The renderer and interaction controller are adapted from the supplied `otis/src/face/` reference. The original reference project is preserved in `otis/` and excluded from the parent application's TypeScript and ESLint scope because it has its own Vite configuration.
