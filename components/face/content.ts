/** Expressions share the same framing so the WebGL mesh can swap textures. */
export const faces = {
  base: {src: '/images/seyi-faces/base.webp', mood: 'Ready to build'},
  l_left: {src: '/images/seyi-faces/l_left.webp', mood: 'Exploring ideas'},
  l_right: {src: '/images/seyi-faces/l_right.webp', mood: 'Looking ahead'},
  x_cross: {src: '/images/seyi-faces/x_cross.webp', mood: 'Debugging…'},
  x_tongue: {src: '/images/seyi-faces/x_tongue.webp', mood: 'A little playful'},
  x_shock: {src: '/images/seyi-faces/x_shock.webp', mood: 'Unexpected input!'},
  x_laugh: {src: '/images/seyi-faces/x_laugh.webp', mood: 'It works!'},
  x_skeptic: {src: '/images/seyi-faces/x_skeptic.webp', mood: 'Checking the assumptions'},
  x_whistle: {src: '/images/seyi-faces/x_whistle.webp', mood: 'Nothing to see here'},
  x_smug: {src: '/images/seyi-faces/x_smug.webp', mood: 'Shipped it'},
  x_dizzy: {src: '/images/seyi-faces/x_dizzy.webp', mood: 'Too many tabs'},
  g_grimace: {src: '/images/seyi-faces/g_grimace.webp', mood: 'Under a little pressure'},
  g_ouch: {src: '/images/seyi-faces/g_ouch.webp', mood: 'Easy on the ideas!'},
  g_squish: {src: '/images/seyi-faces/g_squish.webp', mood: 'Stretching the brief'},
} as const;
export type FaceKey = keyof typeof faces;
export const releaseFaces: FaceKey[] = ['x_tongue', 'x_laugh', 'x_skeptic', 'x_whistle', 'x_smug', 'x_cross'];
export const rig = {
  disc: {x: 0.5, y: 0.54, r: 0.38},
  nose: {x: 0.5, y: 0.46, r: 0.04},
  cheeks: [{x: 0.40, y: 0.49}, {x: 0.60, y: 0.49}],
  face: {x: 0.5, y: 0.43, rx: 0.21, ry: 0.25},
  hairLine: 0.25,
  sides: 0.22,
};
