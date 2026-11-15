/** Split the generated expression sheet into registered WebGL textures. */
import sharp from 'sharp';
import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const directoryName = path.dirname(fileURLToPath(import.meta.url));

async function prepare() {
  const source = process.argv[2];
  if (!source) throw new Error('Pass the generated 4 × 4 PNG sheet path.');
  const directory = path.join(directoryName, '../public/images/seyi-faces');
  await fs.mkdir(directory, {recursive: true});
  const {width, height} = await sharp(source).metadata();
  const keys = ['base', 'l_left', 'l_right', 'x_cross', 'x_tongue', 'x_shock', 'x_laugh', 'x_skeptic', 'x_whistle', 'x_smug', 'x_dizzy', 'g_grimace', 'g_ouch', 'g_squish', 'happy', 'neutral'];
  await fs.copyFile(source, path.join(directory, 'expression-sheet.png'));
  for (let i = 0; i < keys.length; i++) {
    const x = i % 4, y = Math.floor(i / 4);
    const left = Math.round(x * width / 4), top = Math.round(y * height / 4);
    const tile = await sharp(source).extract({left, top, width: Math.round((x + 1) * width / 4) - left, height: Math.round((y + 1) * height / 4) - top}).resize(430, 430).toBuffer();
    await sharp({create: {width: 512, height: 512, channels: 4, background: {r: 0, g: 0, b: 0, alpha: 0}}}).composite([{input: tile, left: 41, top: 41}]).webp({quality: 90}).toFile(path.join(directory, `${keys[i]}.webp`));
  }
  console.log('Prepared 16 character expressions in public/images/seyi-faces');
}
prepare().catch(error => { console.error(error); process.exitCode = 1; });
