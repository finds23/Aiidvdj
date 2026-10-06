/**
 * AllCalidad (allcalidad.re) - plugin para Nuvio
 * Peliculas y series en Latino / Castellano / Subtitulado.
 * Flujo: TMDB -> busqueda en la API REST del sitio (/api/rest/search)
 *        -> series: /api/rest/episodes?post_id=<id> -> id del episodio (temporada x episodio)
 *        -> /api/rest/player?post_id=<id> -> lista "embeds" [{url, lang, quality}]
 *        -> extractor por servidor (ENABLED_SOURCES) -> streams ordenados por idioma
 *        -> ultima entrada de la lista: "ESTADO DE REPRODUCTORES" (no reproducible) que dice
 *           que servidores respondieron (OK), cuales fallaron (y por que) y cuales estan desactivados.
 * Nota: los extractores vienen del plugin allcalidad original; Streamtape usa el de AnimeJara.
 */
var CryptoJS = null;
try { CryptoJS = require('crypto-js'); } catch (e) { /* sin crypto-js: Filemoon y Upnshare se marcan como fallo, el resto sigue */ }

var VERSION = "1.0.0"; // se muestra en el panel de estado para saber que copia carga Nuvio
var TMDB_KEY = "56db0ec297530920213e1503706b81ff";
var FUENTE = "AllCalidad";
var BASE = "https://allcalidad.re";
var API = BASE + "/api/rest";
var CABECERAS = { Accept: "application/json, text/plain, */*", Referer: BASE + "/" };

// Activa/desactiva servidores. Los marcados "probado" se vieron funcionar en otros plugins tuyos;
// "portado" = viene del plugin allcalidad original y aun no se ha probado aqui.
var ENABLED_SOURCES = {
  Streamtape: true,  // probado en JKAnime (extractor de AnimeJara)
  Streamhg: true,    // probado en JKAnime (familia StreamWish)
  Mp4upload: true,   // portado de Latanime / AnimeJara
  Voe: true,         // portado de allcalidad (version con respaldo del script cargador)
  Vidhide: true,     // portado de allcalidad
  Lulustream: true,  // portado de allcalidad
  Uqload: true,      // portado de allcalidad
  Yourupload: true,  // portado de allcalidad
  Okru: true,        // portado de allcalidad (soporta metadataUrl)
  Filemoon: true,    // portado de allcalidad (API cifrada AES-GCM, requiere crypto-js)
  Upnshare: true,    // portado de allcalidad (familia Vidstack/Rpmvid, requiere crypto-js)
  Doodstream: true,  // portado de allcalidad
  Mixdrop: true,     // portado de allcalidad
  Vidmoly: true,     // portado de allcalidad
  Goodstream: true,  // portado de allcalidad
  Vimeos: true,      // portado de allcalidad
  Earnvids: true     // portado de allcalidad
};
// Orden en que aparecen los servidores dentro de cada idioma
var SERVER_ORDER = ["Streamtape", "Mp4upload", "Streamhg", "Voe", "Vidhide", "Lulustream", "Uqload", "Yourupload", "Okru", "Filemoon", "Upnshare", "Doodstream", "Mixdrop", "Vidmoly", "Goodstream", "Vimeos", "Earnvids"];
// Embeds de servicios que ya no existen: se descartan sin gastar tiempo
var DESCARTADOS = /sbcom|lvturbo|vanfem|fembed|1fichier|fireload/i;

// Panel "ESTADO DE REPRODUCTORES" al final de la lista (entrada no reproducible).
var SHOW_STATUS = true;
// Mientras se prueba el plugin: agrega al panel los pasos internos cuando algo falla o no hay resultados.
// Poner en false cuando todo funcione.
var DEBUG = true;
var TRACE = [];
var ESTADO = {};
var ORDEN_ESTADO = [];
function trace(msg) { TRACE.push(String(msg).replace(/\s+/g, " ").slice(0, 140)); }
function shortErr(e) { return String((e && e.message) || e).replace(/ en https?:\/\/\S+/, ""); }

// ---------- utilidades y extractores portados de allcalidad ----------
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36';

const PRESUPUESTO = 42000;

const CIERRE = 50000;

let inicio = Date.now();

function esperar(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function transcurrido() {
  return Date.now() - inicio;
}

function restante() {
  return PRESUPUESTO - transcurrido();
}

function conLimite(promesa, ms, valor) {
  let listo = false;
  const fin = Date.now() + ms;
  const trabajo = Promise.resolve(promesa).then((v) => {
    listo = true;
    return v;
  }, () => {
    listo = true;
    return valor;
  });
  const reloj = (async () => {
    while (!listo && Date.now() < fin) await esperar(Math.max(1, Math.min(250, fin - Date.now())));
    return valor;
  })();
  return Promise.race([trabajo, reloj]);
}

async function traer(url, opciones) {
  if (typeof __native_fetch !== 'function') return fetch(url, opciones);
  const cabeceras = {};
  for (const k of Object.keys(opciones.headers || {})) cabeceras[k] = String(opciones.headers[k]);
  const cuerpo = opciones.body === undefined || opciones.body === null ? null : String(opciones.body);
  const crudo = await __native_fetch(url, String(opciones.method || 'GET').toUpperCase(), JSON.stringify(cabeceras), cuerpo === null ? 'none' : 'text', cuerpo || '', opciones.redirect !== 'manual');
  const d = JSON.parse(crudo);
  const h = d.headers || {};
  return {
    ok: !!d.ok,
    status: d.status,
    statusText: d.statusText,
    url: d.url || url,
    headers: { get: (n) => h[String(n).toLowerCase()] || null },
    text: () => Promise.resolve(d.body || ''),
    json: () => {
      try {
        return Promise.resolve(d.body ? JSON.parse(d.body) : null);
      } catch (e) {
        return Promise.resolve(null);
      }
    }
  };
}

const caidos = new Set();

async function pedir(url, opciones) {
  const host = String(url || '').replace(/^https?:\/\//i, '').split(/[/?#]/)[0].toLowerCase();
  if (caidos.has(host) || restante() <= 0) return null;
  const o = Object.assign({}, opciones || {});
  const limite = Math.min(o.limite || CIERRE, CIERRE - transcurrido());
  delete o.limite;
  o.headers = Object.assign({
    'User-Agent': UA,
    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8'
  }, o.headers || {});
  const r = await conLimite(traer(url, o).catch(() => ({ status: 0 })), limite, null);
  if (r && !r.status) {
    caidos.add(host);
    return null;
  }
  return r;
}

async function texto(url, opciones) {
  const r = await pedir(url, opciones);
  if (!r || !r.ok) return '';
  try {
    return (await conLimite(r.text(), 10000, '')) || '';
  } catch (e) {
    return '';
  }
}

async function json(url, opciones) {
  const t = await texto(url, opciones);
  if (!t) return null;
  try {
    return JSON.parse(t);
  } catch (e) {
    return null;
  }
}

function origen(url) {
  const m = String(url || '').match(/^(https?:\/\/[^/?#]+)/i);
  return m ? m[1] : '';
}

function dominio(url) {
  const m = String(url || '').match(/^(?:https?:)?\/\/([^/?#:]+)/i);
  return m ? m[1].toLowerCase() : '';
}

const ENTIDADES = { amp: '&', quot: '"', apos: "'", lt: '<', gt: '>', nbsp: ' ', ntilde: 'ñ', Ntilde: 'Ñ', aacute: 'á', eacute: 'é', iacute: 'í', oacute: 'ó', uacute: 'ú', Aacute: 'Á', Eacute: 'É', Iacute: 'Í', Oacute: 'Ó', Uacute: 'Ú', uuml: 'ü', iexcl: '¡', iquest: '¿' };

function entidades(t) {
  return String(t || '')
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, d) => String.fromCharCode(parseInt(d, 10)))
    .replace(/&([a-z]+);/gi, (x, n) => (n in ENTIDADES ? ENTIDADES[n] : x));
}

function absoluta(url, base) {
  if (!url) return '';
  const u = entidades(String(url).trim().replace(/\\\//g, '/'));
  if (/^https?:\/\//i.test(u)) return u;
  if (u.startsWith('//')) return `https:${u}`;
  if (u.startsWith('/')) return origen(base) + u;
  const b = String(base || '').replace(/[?#].*$/, '');
  return (/^https?:\/\/[^/]+$/i.test(b) ? `${b}/` : b.replace(/[^/]*$/, '')) + u;
}

function normalizar(t) {
  return String(t || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/&/g, ' y ').replace(/[^a-z0-9]+/g, ' ').trim();
}

function desempacar(html) {
  const salida = [];
  const patron = /eval\(function\(p,a,c,k,e,[a-z]\)\{[\s\S]*?\}\s*\(\s*'((?:[^'\\]|\\.)*)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*'((?:[^'\\]|\\.)*)'\.split\('\|'\)/g;
  const digitos = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
  let m;
  while ((m = patron.exec(String(html || '')))) {
    const base = parseInt(m[2], 10);
    const palabras = m[4].split('|');
    const valor = (t) => {
      let n = 0;
      for (const ch of t) {
        const v = digitos.indexOf(ch);
        if (v < 0 || v >= base) return -1;
        n = n * base + v;
      }
      return n;
    };
    salida.push(m[1].replace(/\\'/g, "'").replace(/\b\w+\b/g, (t) => {
      const i = valor(t);
      return i >= 0 && i < palabras.length && palabras[i] ? palabras[i] : t;
    }));
  }
  return salida.join('\n');
}

function etiquetaAltura(alto) {
  const h = parseInt(alto, 10) || 0;
  if (!h) return '';
  if (h >= 2000) return '4K';
  if (h >= 1400) return '1440p';
  if (h >= 1000) return '1080p';
  if (h >= 700) return '720p';
  if (h >= 470) return '480p';
  if (h >= 350) return '360p';
  return `${h}p`;
}

function calidadTexto(t) {
  const s = String(t || '');
  if (/2160|4k|uhd/i.test(s)) return '4K';
  const m = s.match(/(1440|1080|720|480|360|240)\s*p?/i);
  return m ? `${m[1]}p` : '';
}

async function calidadHls(url, headers) {
  if (!/m3u8|\/hls|master|playlist|\.txt/i.test(url) || restante() < 8000) return '';
  const t = await texto(url, { headers, limite: 5000 });
  let alto = 0;
  const patron = /RESOLUTION=\d+x(\d+)/gi;
  let m;
  while ((m = patron.exec(t))) alto = Math.max(alto, parseInt(m[1], 10));
  return etiquetaAltura(alto);
}

function enlace(url, servidor, headers, calidad) {
  if (!url || !/^https?:\/\//i.test(url)) return [];
  return [{ url, servidor, headers: headers || {}, calidad: calidad || '' }];
}

function buscarVideo(t, base) {
  const fuentes = [
    /["']?hls[24]["']?\s*:\s*["']([^"']+)["']/i,
    /sources\s*:\s*\[\s*\{\s*(?:src|file)\s*:\s*["']([^"']+)["']/i,
    /file\s*:\s*["']([^"']+\.(?:m3u8|mp4|txt)[^"']*)["']/i,
    /["']file["']\s*:\s*["']([^"']+\.(?:m3u8|mp4)[^"']*)["']/i,
    /sources\s*:\s*\[\s*["']([^"']+\.(?:m3u8|mp4)[^"']*)["']/i,
    /src\s*:\s*["']([^"']+\.(?:m3u8|mp4)[^"']*)["']/i,
    /["'](https?:\/\/[^"'\s]+\.m3u8[^"'\s]*)["']/i,
    /["'](https?:\/\/[^"'\s]+\.mp4[^"'\s]*)["']/i
  ];
  for (const f of fuentes) {
    const m = String(t || '').match(f);
    if (m) return absoluta(m[1], base);
  }
  return '';
}

function atobSeguro(t) {
  try {
    return atob(String(t || '').replace(/\s+/g, ''));
  } catch (e) {
    return '';
  }
}

function descifrarVoe(cifrado, ruidos) {
  let t = cifrado.replace(/[a-zA-Z]/g, (c) => {
    const tope = c <= 'Z' ? 90 : 122;
    const n = c.charCodeAt(0) + 13;
    return String.fromCharCode(n <= tope ? n : n - 26);
  });
  for (const ruido of ruidos || ['@$', '^^', '~@', '%?', '*~', '!!', '#&']) t = t.split(ruido).join('');
  const paso = atobSeguro(t);
  if (!paso) return null;
  let movido = '';
  for (let i = 0; i < paso.length; i++) movido += String.fromCharCode(paso.charCodeAt(i) - 3);
  const final = atobSeguro(movido.split('').reverse().join(''));
  try {
    return JSON.parse(final);
  } catch (e) {
    return null;
  }
}

async function resolverVoe(url, referer) {
  let actual = url;
  let html = '';
  for (let i = 0; i < 3; i++) {
    html = await texto(actual, { headers: { Referer: i === 0 && referer ? referer : actual } });
    const salto = html.length < 4000 && html.match(/window\.location\.href\s*=\s*['"]([^'"]+)['"]/i);
    if (!salto) break;
    actual = absoluta(salto[1], actual);
  }
  if (!html) return [];
  const bloque = html.match(/<script type="application\/json">([\s\S]*?)<\/script>(?:\s*<script[^>]*src=["']([^"']+)["'])?/i);
  if (bloque) {
    let cifrado = '';
    try {
      const dato = JSON.parse(bloque[1].trim());
      cifrado = Array.isArray(dato) ? dato[0] : dato;
    } catch (e) {}
    let datos = cifrado ? descifrarVoe(cifrado) : null;
    if (!datos && cifrado && bloque[2]) {
      const cargador = await texto(absoluta(bloque[2], actual), { headers: { Referer: actual } });
      const lista = (cargador.match(/\[(?:\s*'[^']{1,10}'\s*,?){4,12}\]/) || cargador.match(/\[(?:\s*"[^"]{1,10}"\s*,?){4,12}\]/) || [])[0];
      if (lista) datos = descifrarVoe(cifrado, (lista.match(/['"]([^'"]{1,10})['"]/g) || []).map((x) => x.slice(1, -1)));
    }
    const video = datos && (datos.source || datos.direct_access_url);
    if (video) return enlace(video, 'Voe', { Referer: actual, 'User-Agent': UA });
  }
  const directo = html.match(/['"]hls['"]\s*:\s*['"]([^'"]+)['"]/i);
  if (directo) {
    const v = /^aHR0/.test(directo[1]) ? atobSeguro(directo[1]) : directo[1];
    return enlace(v, 'Voe', { Referer: actual, 'User-Agent': UA });
  }
  return enlace(buscarVideo(html, actual), 'Voe', { Referer: actual, 'User-Agent': UA });
}

async function resolverEmpaquetado(url, servidor, referer, siguiendo) {
  const propio = `${origen(url)}/`;
  for (const ref of [...new Set([referer || propio, propio])]) {
    const html = await texto(url, { headers: { Referer: ref } });
    if (!html) continue;
    const codigo = `${desempacar(html)}\n${html}`;
    const links = codigo.match(/links\s*=\s*(\{[^}]+\})/);
    let video = '';
    if (links) {
      try {
        const o = JSON.parse(links[1].replace(/'/g, '"'));
        video = o.hls4 || o.hls3 || o.hls2 || o.hls || '';
      } catch (e) {}
    }
    video = absoluta(video || buscarVideo(codigo, url), url);
    if (video) return enlace(video, servidor, { Referer: propio, Origin: origen(url), 'User-Agent': UA });
    const marco = html.match(/<iframe[^>]+src=["']([^"']+)["']/i);
    if (marco && !siguiendo) return resolverEmpaquetado(absoluta(marco[1], url), servidor, url, true);
  }
  return [];
}

async function resolverStreamwish(url, referer) {
  const propio = await resolverEmpaquetado(url, 'StreamWish', referer);
  if (propio.length) return propio;
  const id = url.replace(/[?#].*$/, '').split('/').filter(Boolean).pop().replace(/\.html$/, '');
  const espejos = [`https://hglink.to/e/${id}`, `https://streamwish.to/e/${id}`, `https://vibuxer.com/e/${id}`].filter((e) => dominio(e) !== dominio(url));
  const listas = await Promise.all(espejos.map((e) => resolverEmpaquetado(e, 'StreamWish', referer, true)));
  return listas.find((l) => l.length) || [];
}

function bytesBase64Url(t) {
  let b = String(t || '').replace(/-/g, '+').replace(/_/g, '/');
  while (b.length % 4) b += '=';
  return CryptoJS.enc.Base64.parse(b);
}

async function resolverByse(url) {
  const base = origen(url);
  const codigo = url.replace(/[?#].*$/, '').replace(/\/+$/, '').split('/').pop();
  const detalle = await json(`${base}/api/videos/${codigo}/embed/details`, { headers: { Referer: url, 'X-Requested-With': 'XMLHttpRequest' } });
  const marco = detalle && detalle.embed_frame_url;
  if (!marco) return resolverEmpaquetado(url, 'Filemoon');
  const baseMarco = origen(marco);
  const codigoMarco = marco.replace(/[?#].*$/, '').replace(/\/+$/, '').split('/').pop();
  const play = await json(`${baseMarco}/api/videos/${codigoMarco}/embed/playback`, {
    headers: { Accept: '*/*', Referer: marco, 'X-Embed-Parent': url, 'Accept-Language': 'en-US,en;q=0.5' }
  });
  const p = play && play.playback;
  if (!p || !p.key_parts || !p.payload || !p.iv) return [];
  try {
    const clave = bytesBase64Url(p.key_parts[0]).concat(bytesBase64Url(p.key_parts[1]));
    const claro = CryptoJS.AES.decrypt({ ciphertext: bytesBase64Url(p.payload) }, clave, { iv: bytesBase64Url(p.iv), mode: CryptoJS.mode.GCM, padding: CryptoJS.pad.NoPadding }).toString(CryptoJS.enc.Utf8);
    const datos = JSON.parse(claro.replace(/^\uFEFF/, ''));
    const fuente = datos.sources && datos.sources[0];
    return enlace(fuente && fuente.url, 'Filemoon', { Referer: `${base}/`, 'User-Agent': UA }, calidadTexto(fuente && fuente.label));
  } catch (e) {
    return [];
  }
}

async function resolverDood(url, referer) {
  const embed = url.replace(/\/(d|f|download)\//, '/e/');
  const html = await texto(embed, { headers: { Referer: referer || embed } });
  const m = html.match(/\/pass_md5\/[\w-]+\/([\w-]+)/);
  if (!m) return [];
  const base = origen(embed);
  const prefijo = await texto(base + m[0], { headers: { Referer: embed } });
  if (!/^https?:/.test(prefijo)) return [];
  let azar = '';
  const letras = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < 10; i++) azar += letras[Math.floor(Math.random() * letras.length)];
  return enlace(`${prefijo}${azar}?token=${m[1]}&expiry=${Date.now()}`, 'Doodstream', { Referer: `${base}/`, 'User-Agent': UA });
}

async function resolverStreamtape(url) {
  const html = await texto(url.replace('/v/', '/e/'));
  const m = html.match(/getElementById\(['"](?:robotlink|ideoooolink|botlink)['"]\)\.innerHTML\s*=\s*['"]([^'"]+)['"]\s*\+\s*\(?['"]([^'"]+)['"]\)?(?:\.substring\((\d+)\))?(?:\.substring\((\d+)\))?/);
  if (!m) return [];
  let resto = m[2];
  if (m[3]) resto = resto.substring(parseInt(m[3], 10));
  if (m[4]) resto = resto.substring(parseInt(m[4], 10));
  return enlace(`https:${m[1]}${resto}&stream=1`.replace('https:https:', 'https:'), 'Streamtape', { Referer: 'https://streamtape.com/', 'User-Agent': UA }, '');
}

async function resolverUqload(url, referer) {
  const embed = /embed-/.test(url) ? url : url.replace(/\.(?:com|co|io|net|ws|to|cx|bz)\/(?!embed-)/, (s) => `${s}embed-`);
  const html = await texto(embed, { headers: { Referer: referer || embed } });
  const m = html.match(/sources\s*:\s*\[\s*["']([^"']+)["']/);
  return enlace(m && m[1], 'Uqload', { Referer: `${origen(embed)}/`, 'User-Agent': UA });
}

async function resolverOkru(url) {
  const id = (url.match(/(?:videoembed|video)\/([\d-]+)/) || [])[1];
  if (!id) return [];
  const html = await texto(`https://ok.ru/videoembed/${id}`);
  const m = html.match(/data-options=(["'])(\{[\s\S]+?\})\1/);
  if (!m) return [];
  const cab = { Referer: 'https://ok.ru/', 'User-Agent': UA };
  try {
    const opciones = JSON.parse(entidades(m[2]));
    if (opciones.isExternalPlayer) return [];
    const variables = opciones.flashvars || {};
    let meta = null;
    if (variables.metadata) {
      meta = typeof variables.metadata === 'string' ? JSON.parse(variables.metadata) : variables.metadata;
    } else if (variables.metadataUrl) {
      meta = await json(decodeURIComponent(variables.metadataUrl), {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', Referer: 'https://ok.ru/' },
        body: variables.location ? `st.location=${encodeURIComponent(variables.location)}` : ''
      });
    }
    if (!meta) return [];
    const orden = { ultra: '4K', quad: '1440p', full: '1080p', hd: '720p', sd: '480p', low: '360p', lowest: '240p', mobile: '144p' };
    const videos = (meta.videos || []).filter((v) => v.url && orden[v.name]);
    if (videos.length) {
      videos.sort((a, b) => pesoOk(orden[b.name]) - pesoOk(orden[a.name]));
      return enlace(absoluta(videos[0].url, 'https://ok.ru/'), 'OK.ru', cab, orden[videos[0].name]);
    }
    return enlace(meta.hlsManifestUrl || meta.ondemandHls, 'OK.ru', cab);
  } catch (e) {
    return [];
  }
}

function pesoOk(c) {
  return c === '4K' ? 2160 : parseInt(c, 10) || 0;
}

async function resolverVimeos(url, referer) {
  const embed = /embed-/.test(url) ? url : url.replace(/vimeos\.net\//, 'vimeos.net/embed-');
  const casa = origen(embed);
  const cabeceras = { Referer: `${casa}/`, Origin: casa, 'User-Agent': UA };
  let ultimo = '';
  for (let intento = 0; intento < 3; intento++) {
    const html = await texto(embed, { headers: { Referer: referer || 'https://la.movie/tv/' } });
    const codigo = `${desempacar(html)}\n${html}`;
    const video = (codigo.match(/file\s*:\s*["']([^"']+\.m3u8[^"']*)["']/) || codigo.match(/["'](https?:\/\/[^"']+\.m3u8[^"']*)["']/) || [])[1];
    if (video) {
      ultimo = absoluta(video, embed);
      if (!/[?&]i=/.test(ultimo) || /[?&]i=0\.0(&|$)/.test(ultimo)) break;
    }
    await esperar(400);
  }
  return enlace(ultimo, 'Vimeos', cabeceras);
}

async function resolverMixdrop(url, referer) {
  const embed = url.replace('/f/', '/e/');
  const html = await texto(embed, { headers: { Referer: referer || embed } });
  const codigo = desempacar(html);
  const m = codigo.match(/MDCore\.wurl\s*=\s*["']([^"']+)["']/);
  return enlace(m && absoluta(m[1], embed), 'Mixdrop', { Referer: `${origen(embed)}/`, 'User-Agent': UA });
}

async function resolverVidmoly(url) {
  const html = await texto(url, { headers: { Referer: 'https://vidmoly.me/' } });
  const marco = html.match(/<iframe[^>]+src=["']([^"']*(?:embed-|vidmoly\.biz)[^"']*)["']/i);
  const pagina = marco ? await texto(absoluta(marco[1], url), { headers: { Referer: url } }) : html;
  const m = pagina.match(/file\s*:\s*["']([^"']+)["']/);
  return enlace(m && m[1], 'VidMoly', { Referer: 'https://vidmoly.me/', 'User-Agent': UA });
}

const COMPLEMENTO_RPM = '\nfunction re(n,e){const t=sa();return re=function(s,i){return s=s-109,t[s]},re(n,e)}\nfunction p(...g){return String.fromCodePoint(...g)}\nfunction v(g,b){return g.codePointAt(b)||0}\nfunction S(g){return __utf8Encode(g)}\nT=()=>{const g=re,b=window[g(263)][g(585)],P="10",k=110,U=1;let M="";const B=v("\u1D5F")[g(321)]()[g(199)]("");for(let de=0;de<B.length;de++)M+=p(P+B[de]);M+=p(v(b,P/10)),M+=M[g(336)](1,3),M+=p(k,k-1,k+7);const se=g(370)[g(199)]("");return M+=p(se[3]+se[2],se[1]+se[2]),M+=p(se[0]*U+U+se[3],se[0]*U+U+se[3]),M+=p(se[3]*P+se[3]*U,se[g(580)]()[g(364)]("")[g(336)](0,2)),S(M)}\nC=()=>{const g=re,b=window[g(263)][g(585)],P=b+"//",k=window.location[g(217)],U=b[g(316)]*P[g(316)],M=1;let B="";for(let me=M;me<10;me++)B+=p(me+U);let se="";se=M+se+M+se+M;const de=se[g(316)]*v(k),Ie=se*M+b.length,I=Ie+4,j=v(b,M),oe=j*M-2;return B+=p(U,se,de,Ie,I,j,oe),S(B)}\n';

const codigoRpm = {};

function bytesUtf8(t) {
  const bytes = [];
  for (let i = 0; i < t.length; i++) {
    const c = t.codePointAt(i);
    if (c > 65535) i++;
    if (c < 128) bytes.push(c);
    else if (c < 2048) bytes.push(192 | (c >> 6), 128 | (c & 63));
    else if (c < 65536) bytes.push(224 | (c >> 12), 128 | ((c >> 6) & 63), 128 | (c & 63));
    else bytes.push(240 | (c >> 18), 128 | ((c >> 12) & 63), 128 | ((c >> 6) & 63), 128 | (c & 63));
  }
  return bytes;
}

function hexDe(bytes) {
  return bytes.map((b) => (`0${(b & 255).toString(16)}`).slice(-2)).join('');
}

async function clavesRpm(base, hash) {
  if (!codigoRpm[base]) {
    const html = await texto(`${base}/`);
    const ruta = (html.match(/src=["'](\/assets\/index-[\w-]+\.js)["']/) || [])[1];
    if (!ruta) return null;
    const js = await texto(`${base}${ruta}`, { headers: { Referer: `${base}/` } });
    const inicio = js.indexOf('function sa(){');
    if (inicio < 0) return null;
    const fin = js.slice(inicio).match(/}\)\(sa,\s*\d+\s*\);/);
    if (!fin) return null;
    codigoRpm[base] = js.slice(inicio, inicio + fin.index + fin[0].length) + COMPLEMENTO_RPM;
  }
  try {
    const ventana = { location: { protocol: 'https:', hash: `#${hash}` } };
    const r = new Function('window', '__utf8Encode', 'parseInt', 'String', `${codigoRpm[base]}\nreturn { T: T(), C: C() };`)(ventana, bytesUtf8, parseInt, String);
    return { clave: CryptoJS.enc.Hex.parse(hexDe(r.T.slice(0, 16))), vector: CryptoJS.enc.Hex.parse(hexDe(r.C.slice(0, 16))) };
  } catch (e) {
    return null;
  }
}

function abrirHexRpm(cifrado, clave, vector) {
  try {
    const claro = CryptoJS.AES.decrypt({ ciphertext: CryptoJS.enc.Hex.parse(cifrado) }, clave, { iv: vector, mode: CryptoJS.mode.CBC, padding: CryptoJS.pad.Pkcs7 }).toString(CryptoJS.enc.Utf8);
    return claro && claro.includes('{') ? claro : '';
  } catch (e) {
    return '';
  }
}

async function listaViva(url, headers) {
  const maestro = await texto(url, { headers });
  if (!/#EXTM3U/i.test(maestro)) return false;
  const hijo = maestro.split(/\r?\n/).find((l) => l.trim() && !l.startsWith('#'));
  if (!hijo) return true;
  return /#EXTM3U/i.test(await texto(absoluta(hijo.trim(), url), { headers }));
}

async function resolverVidstack(url, referer) {
  const base = origen(url);
  const id = url.includes('#') ? url.split('#').pop().replace(/^\//, '').split('&')[0] : ((url.match(/[?&]id=([^&#]+)/) || [])[1] || url.replace(/[?#].*$/, '').split('/').filter(Boolean).pop());
  const fija = CryptoJS.enc.Utf8.parse('kiemtienmua911ca');
  const cabeceras = { Referer: `${base}/`, Origin: base, 'User-Agent': UA };
  const hex = (await texto(`${base}/api/v1/video?id=${encodeURIComponent(id)}&w=1920&h=1080&r=${dominio(referer || '')}`, { headers: cabeceras })).trim();
  let claro = '';
  if (/^[0-9a-f]+$/i.test(hex)) {
    claro = abrirHexRpm(hex, fija, CryptoJS.enc.Utf8.parse('1234567890oiuytr')) || abrirHexRpm(hex, fija, CryptoJS.enc.Utf8.parse('0123456789abcdef'));
    if (!claro) {
      const dinamicas = await clavesRpm(base, id);
      if (dinamicas) claro = abrirHexRpm(hex, dinamicas.clave, dinamicas.vector);
    }
  }
  if (!claro) {
    const r = await json(`${base}/api/v1/video`, {
      method: 'POST',
      headers: Object.assign({ 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', 'X-Requested-With': 'XMLHttpRequest' }, cabeceras),
      body: `url=${encodeURIComponent(id)}`
    });
    if (r && r.payload) {
      try {
        claro = CryptoJS.AES.decrypt(r.payload, fija, { iv: CryptoJS.enc.Utf8.parse('1234567890oiuytr'), mode: CryptoJS.mode.CBC, padding: CryptoJS.pad.Pkcs7 }).toString(CryptoJS.enc.Utf8);
      } catch (e) {}
    }
  }
  if (!claro) return [];
  let d = {};
  try {
    d = JSON.parse(claro);
  } catch (e) {
    d = { source: ((claro.match(/"source"\s*:\s*"([^"]+)"/) || [])[1] || '') };
  }
  const candidatos = [d.source, d.cfNative, d.hlsVideoTiktok, d.url, d.sources && d.sources[0] && d.sources[0].file]
    .filter(Boolean)
    .map((v) => absoluta(String(v).replace(/\\\//g, '/'), base))
    .map((v) => (/\.txt(\?|$)/.test(v) ? `${v}#index.m3u8` : v));
  const unicos = [...new Set(candidatos)];
  if (unicos.length > 1) {
    for (const c of unicos) {
      if (await listaViva(c, cabeceras)) return enlace(c, 'Rpmvid', cabeceras);
    }
  }
  return enlace(unicos[0], 'Rpmvid', cabeceras);
}

async function resolverYourupload(url) {
  const html = await texto(url.replace('/watch/', '/embed/'));
  const m = html.match(/file\s*:\s*['"]([^'"]+)['"]/) || html.match(/property=["']og:video["'][^>]+content=["']([^"']+)["']/i);
  return enlace(m && m[1], 'YourUpload', { Referer: 'https://www.yourupload.com/', 'User-Agent': UA });
}

const CALIDADES_URLSET = {
  vimeos: { h: '720p', n: '480p' },
  goodstream: { x: '1080p', h: '720p', n: '480p', l: '360p' },
  vidhide: { n: '720p', l: '480p' },
  wish: { x: '1080p', h: '1080p', n: '720p', l: '480p' },
  voe: { n: '720p', l: '360p' }
};

function calidadUrlset(url) {
  const familia = /vimeos/.test(url) ? 'vimeos' : /goodstream/.test(url) ? 'goodstream' : /cloudwindow-route/.test(url) ? 'voe' : /minochinos|vidhide|dintezuvio|dramiyos/.test(url) ? 'vidhide' : /premilkyway|hlswish|vibuxer|streamwish/.test(url) ? 'wish' : '';
  const m = String(url || '').match(/_,([a-z,]+),\.urlset/);
  if (familia && m) {
    const partes = m[1].split(',');
    for (const letra of ['x', 'o', 'h', 'n', 'l']) if (partes.includes(letra) && CALIDADES_URLSET[familia][letra]) return CALIDADES_URLSET[familia][letra];
  }
  return calidadTexto((String(url || '').match(/[_\-/](\d{3,4})p/) || [])[0] || '');
}

async function ligeroWish(url, referer) {
  const destino = url.replace('hglink.to', 'vibuxer.com');
  const casa = origen(destino);
  for (const ref of [...new Set([referer || 'https://embed69.org/', 'https://embed69.org/'])]) {
    const html = await texto(destino, { headers: { Referer: ref, Origin: origen(ref), 'Accept-Language': 'es-MX,es;q=0.9' } });
    if (!html) continue;
    const codigo = `${desempacar(html)}\n${html}`;
    let video = (html.match(/file\s*:\s*["']([^"']+)["']/i) || [])[1] || '';
    if (!video) {
      const bloque = codigo.match(/\{[^{}]*["']?hls[234]["']?\s*:\s*["']([^"']+)["'][^{}]*\}/);
      if (bloque) {
        const opciones = {};
        const par = /["']?(hls[234])["']?\s*:\s*["']([^"']+)["']/g;
        let m;
        while ((m = par.exec(bloque[0]))) opciones[m[1]] = m[2];
        video = opciones.hls4 || opciones.hls3 || opciones.hls2 || '';
      }
    }
    if (!video) video = (codigo.match(/["']([^"']{30,}\.m3u8[^"']*)["']/i) || [])[1] || '';
    if (video) return enlace(absoluta(video, destino), 'StreamWish', { 'User-Agent': UA, Referer: `${casa}/` });
  }
  return [];
}

async function ligeroGoodstream(url) {
  const html = await texto(url, { headers: { Referer: 'https://goodstream.one' } });
  const video = (html.match(/file:\s*"([^"]+)"/) || [])[1];
  return enlace(video, 'GoodStream', { Referer: url, Origin: 'https://goodstream.one', 'User-Agent': UA });
}

async function datosTmdb(tmdbId, tipo) {
  let id = String(tmdbId || '').trim();
  if (/^tt\d+$/.test(id)) {
    const f = await json(`https://api.themoviedb.org/3/find/${id}?api_key=${TMDB_KEY}&external_source=imdb_id`);
    const r = f && (tipo === 'movie' ? f.movie_results : f.tv_results);
    if (!r || !r[0]) return null;
    id = String(r[0].id);
  }
  const ruta = `https://api.themoviedb.org/3/${tipo}/${id}?api_key=${TMDB_KEY}`;
  const [es, en] = await Promise.all([
    json(`${ruta}&language=es-MX&append_to_response=external_ids,alternative_titles`),
    json(`${ruta}&language=en-US`)
  ]);
  if (!es && !en) return null;
  const a = es || {};
  const b = en || {};
  return {
    id,
    titulo: a.title || a.name || b.title || b.name || '',
    ingles: b.title || b.name || '',
    original: a.original_title || a.original_name || b.original_title || b.original_name || '',
    anio: String(a.release_date || a.first_air_date || b.release_date || b.first_air_date || '').slice(0, 4),
    imdb: (a.external_ids && a.external_ids.imdb_id) || a.imdb_id || b.imdb_id || '',
    alternos: ((a.alternative_titles && (a.alternative_titles.results || a.alternative_titles.titles)) || []).map((x) => ({ pais: x.iso_3166_1 || '', titulo: x.title || '' })),
    temporadas: (a.seasons || b.seasons || []).map((x) => ({ numero: Number(x.season_number), episodios: Number(x.episode_count) || 0 }))
  };
}

function encabezado(datos, tipo, temporada, episodio) {
  if (tipo === 'tv') return `${datos.titulo} - T${temporada} E${episodio}`;
  return datos.anio ? `${datos.titulo} (${datos.anio})` : datos.titulo;
}

function titulosPosibles(datos) {
  const vistos = new Set();
  return [datos.titulo, datos.ingles, datos.original].filter((t) => {
    const n = normalizar(t);
    if (!n || vistos.has(n)) return false;
    vistos.add(n);
    return true;
  });
}

const RELLENO = new Set(['ver', 'online', 'gratis', 'latino', 'castellano', 'subtitulado', 'espanol', 'audio', 'hd', 'full', 'completa', 'pelicula', 'serie']);

const VACIAS = new Set(['the', 'and', 'of', 'a', 'el', 'la', 'los', 'las', 'de', 'del', 'y', 'en', 'un', 'una']);

function limpiarTitulo(t) {
  const n = normalizar(String(t || '').replace(/\([^)]*\)|\[[^\]]*\]/g, ' '));
  const limpio = n.split(' ').filter((p) => !RELLENO.has(p)).join(' ');
  return limpio || n;
}

function parecido(a, b) {
  const x = limpiarTitulo(a);
  const y = limpiarTitulo(b);
  if (!x || !y) return 0;
  if (x === y) return 1;
  const [corto, largo] = x.length <= y.length ? [x, y] : [y, x];
  const contiene = ` ${largo} `.includes(` ${corto} `) ? 0.6 + (0.4 * corto.length) / largo.length : 0;
  const px = new Set(x.split(' ').filter((p) => !VACIAS.has(p)));
  const py = new Set(y.split(' ').filter((p) => !VACIAS.has(p)));
  let comunes = 0;
  for (const p of px) if (py.has(p)) comunes++;
  const palabras = px.size && py.size ? comunes / Math.max(px.size, py.size) : 0;
  return Math.max(contiene, palabras);
}

function audioDe(t) {
  const s = normalizar(t);
  if (!s) return '';
  if (/latino|\blat\b|latam|mexic|\bmx\b|419|doblaje latino/.test(s)) return 'Latino';
  if (/castellano|espana|\bcast\b|\besp\b|\bes es\b/.test(s)) return 'Castellano';
  if (/subtitulad|\bvose\b|\bsub\b|\bsubs\b|\bvo\b|japones|ingles|english|original/.test(s)) return 'Subtitulado';
  if (/espanol|spanish/.test(s)) return 'Español';
  return '';
}

function pesoCalidad(c) {
  if (/4k/i.test(c)) return 2160;
  return Number((String(c || '').match(/(\d{3,4})p/i) || [])[1]) || 0;
}

function pesoAudio(a) {
  return { Latino: 3, 'Español': 2, Castellano: 1 }[a] || 0;
}

function limitar(buscador) {
  return (...argumentos) => {
    inicio = Date.now();
    caidos.clear();
    return conLimite(Promise.resolve().then(() => buscador(...argumentos)), CIERRE, []);
  };
}

function consultaCorta(titulo) {
  const q = String(titulo || '').split(':')[0].replace(/\([^)]*\)|\[[^\]]*\]/g, ' ');
  return normalizar(q).split(' ').filter((p) => p.length > 1).slice(0, 2).join(' ');
}

async function buscarCorto(datos, tipo) {
  const clase = tipo === 'movie' ? 'movies' : 'tvshows';
  for (const titulo of titulosPosibles(datos)) {
    const consulta = consultaCorta(titulo);
    if (!consulta) continue;
    const r = await json(`${API}/search?post_type=${clase}&query=${encodeURIComponent(consulta)}&posts_per_page=20`, { headers: CABECERAS });
    const posts = (r && r.data && r.data.posts) || [];
    const palabras = consulta.split(' ').filter((p) => p.length > 2);
    if (!posts.some((p) => palabras.some((w) => normalizar(p.title).includes(w)))) continue;
    const limpio = limpiarTitulo(titulo);
    const coincide = (p) => {
      const a = limpiarTitulo(p.title);
      const b = limpiarTitulo(p.original_title || '');
      return (a && (a.includes(limpio) || limpio.includes(a))) || (b && (b.includes(limpio) || limpio.includes(b)));
    };
    const conAnio = posts.find((p) => {
      if (!coincide(p)) return false;
      const suyo = (String(p.title).match(/\((\d{4})\)/) || [])[1] || String(p.release_date || '').slice(0, 4);
      return !datos.anio || !/^\d{4}$/.test(suyo) || Math.abs(Number(suyo) - Number(datos.anio)) <= 1;
    });
    if (conAnio) return conAnio;
    if (coincide(posts[0])) return posts[0];
  }
  return null;
}

async function buscarPost(datos, tipo) {
  const clases = tipo === 'movie' ? 'movies' : 'tvshows,animes';
  let mejor = null;
  let puntos = 0;
  for (const consulta of titulosPosibles(datos)) {
    let r = await json(`${API}/search?query=${encodeURIComponent(consulta)}&page=1&post_type=${clases}&posts_per_page=24`, { headers: { Accept: '*/*' } });
    if (!r || !r.data || !(r.data.posts || []).length) r = await json(`${API}/search?post_type=movies%2Ctvshows%2Canimes&query=${encodeURIComponent(consulta).replace(/%20/g, '+')}&posts_per_page=16&page=1`, { headers: { Accept: 'application/json', Referer: `${BASE}/` } });
    for (const p of (r && r.data && r.data.posts) || []) {
      if (p.type && (tipo === 'movie') !== (p.type === 'movies')) continue;
      let s = Math.max(parecido(p.title, consulta), parecido(p.title, datos.titulo), parecido(p.title, datos.original));
      const anio = String(p.release_date || '').slice(0, 4);
      if (datos.anio && /^\d{4}$/.test(anio)) s += anio === datos.anio ? 0.2 : Math.abs(Number(anio) - Number(datos.anio)) > 1 ? -0.2 : 0;
      if (s > puntos) {
        puntos = s;
        mejor = p;
      }
    }
    if (puntos >= 1.1) break;
  }
  return puntos >= 0.6 ? mejor : null;
}

// Streamtape (extractor de AnimeJara, probado)
function extractStreamtapeFromHtml(html) {
  var re = /getElementById\('(?:robotlink|botlink)'\)\.innerHTML\s*=\s*['"]([^'"]*)['"]\s*\+\s*(?:''\s*\+\s*)?\(\s*['"]([^'"]*)['"]\s*\)((?:\.substring\(\d+\))*)/g;
  var m, last = null;
  while ((m = re.exec(html)) !== null) last = m; // 'ideoolink' son senuelos; robotlink/botlink dan el enlace valido
  if (!last) return null;
  var tail = last[2];
  (last[3].match(/\.substring\(\d+\)/g) || []).forEach(function (s) { tail = tail.substring(parseInt(/\d+/.exec(s)[0], 10)); });
  var url = last[1] + tail;
  if (url.indexOf("//") === 0) url = "https:" + url;
  return url + "&stream=1";
}

// Tabla de servidores: dominio del embed -> clave de ENABLED_SOURCES -> extractor
// (en esta tabla el orden importa: gana la primera coincidencia)
var SERVIDORES = [
  { key: 'Streamtape', label: 'Streamtape', host: /streamtape|strtape|stape|tapecontent|streamta\.pe|strcloud|streamadblock/i, crypto: false, run: function (u, r) { return resolverStreamtapeCombinado(u, r); } },
  { key: 'Mp4upload', label: 'MP4Upload', host: /mp4upload/i, crypto: false, run: function (u, r) { return resolverMp4upload(u, r); } },
  { key: 'Streamhg', label: 'StreamHG', host: /streamwish|swdyu|wishembed|playerwish|strwish|swhoi|wishfast|sfastwish|hlswish|embedwish|awish|dwish|streamhg|hglink|habetar|mwish|kswplayer|swiftplayers|hanerix|cdnwish|flaswish|obeywish|davioad|jodwish|ghbrisk|dhcplay|iplayerhls|cybervynx|dumbalag|wishonly|streamwishplayer|asnwish|nekowish|neko-stream|multimovies|streamhls|vibuxer|hlswish/i, crypto: false, run: function (u, r) { return ligeroWish(u, r).then(function (p) { return p.length ? p : resolverStreamwish(u, r); }); } },
  { key: 'Voe', label: 'VOE', host: /voe\.sx|voe-unblock|voeunbl|voeun|v-o-e|voe\./i, crypto: false, run: function (u, r) { return resolverVoe(u, r); } },
  { key: 'Vidhide', label: 'Vidhide', host: /vidhide|filelions|ryderjet|dintezuvio|mivalyo|dhtpre|peytonepre|smoothpre|vidhidepre|louishide|lylxan|movearnpre|kinoger|alions|azipcdn|nikaplayer|fviplions|vidhidevip|niikaplayerr|callistanise|dinisglows|vidhideplus|vidhidehub|dingtezuni|minochinos/i, crypto: false, run: function (u, r) { return resolverEmpaquetado(u, 'Vidhide', r); } },
  { key: 'Lulustream', label: 'Lulustream', host: /lulustream|luluvdo|lulu\.st|luluvid/i, crypto: false, run: function (u, r) { return resolverEmpaquetado(u, 'Lulustream', r); } },
  { key: 'Uqload', label: 'Uqload', host: /uqload|uqloads/i, crypto: false, run: function (u, r) { return resolverUqload(u, r); } },
  { key: 'Yourupload', label: 'YourUpload', host: /yourupload/i, crypto: false, run: function (u, r) { return resolverYourupload(u); } },
  { key: 'Okru', label: 'Okru', host: /ok\.ru|odnoklassniki/i, crypto: false, run: function (u, r) { return resolverOkru(u); } },
  { key: 'Filemoon', label: 'Filemoon', host: /filemoon|moonplayer|kerapoxy|byse|bysezoxexe|bysezejataos|byse[a-z]*|f16px|filemooon|1azayf|smdfs40r/i, crypto: true, run: function (u, r) { return resolverByse(u); } },
  { key: 'Upnshare', label: 'Upnshare', host: /rpmvid|rpmplay|rpmshare|upnshare|upns\.|vidstack|cubeembed|uns\.bio|p2pplay|4meplayer|p2pstream|strp2p/i, crypto: true, run: function (u, r) { return resolverVidstack(u, r); } },
  { key: 'Doodstream', label: 'Doodstream', host: /dood|d0000d|d000d|ds2play|ds2video|dooood|doods|do0od|vide0|vidply|all3do|dood\.|d-s\.io|dsvplay|myvidplay|playmogo|do7go/i, crypto: false, run: function (u, r) { return resolverDood(u.replace('dsvplay.com', 'd0000d.com'), r); } },
  { key: 'Mixdrop', label: 'Mixdrop', host: /mixdrop|mxdrop|mixdroop|m1xdrop|mdbekjwqa|mdfx9dc8n/i, crypto: false, run: function (u, r) { return resolverMixdrop(u, r); } },
  { key: 'Vidmoly', label: 'Vidmoly', host: /vidmoly/i, crypto: false, run: function (u, r) { return resolverVidmoly(u); } },
  { key: 'Goodstream', label: 'Goodstream', host: /goodstream/i, crypto: false, run: function (u, r) { return ligeroGoodstream(u); } },
  { key: 'Vimeos', label: 'Vimeos', host: /vimeos/i, crypto: false, run: function (u, r) { return resolverVimeos(u, r); } },
  { key: 'Earnvids', label: 'Earnvids', host: /earnvids|earnl\.|vidnova|streamfort/i, crypto: false, run: function (u, r) { return resolverEmpaquetado(u, 'Earnvids', r, true); } }
];

// ---------- extractores propios de esta capa ----------
async function resolverMp4upload(url, referer) {
  var embed = url.replace(/mp4upload\.com\/(?!embed-)/, "mp4upload.com/embed-");
  if (!/\.html(\?|$)/.test(embed)) embed += ".html";
  var html = await texto(embed, { headers: { Referer: referer || BASE + "/" } });
  var pats = [
    /src\s*:\s*["']([^"']+\.mp4[^"']*)["']/i,
    /src\s*=\s*["']([^"']+\.mp4[^"']*)["']/i,
    /file\s*:\s*["']([^"']+\.mp4[^"']*)["']/i
  ];
  var video = "";
  for (var i = 0; i < pats.length && !video; i++) {
    var m = html.match(pats[i]);
    if (m && m[1]) video = m[1].trim();
  }
  return enlace(absoluta(video, embed), "Mp4upload", { Referer: embed, "User-Agent": UA });
}

async function resolverStreamtapeCombinado(url, referer) {
  var embed = url.replace("/v/", "/e/");
  var html = await texto(embed, { headers: { Referer: referer || BASE + "/" } });
  var u = extractStreamtapeFromHtml(html);
  if (u) return enlace(u, "Streamtape", { Referer: "https://streamtape.com/", "User-Agent": UA });
  return resolverStreamtape(url); // respaldo: patron del plugin allcalidad
}

// ---------- panel de estado ----------
function anotar(label, tipo, audio, enlaces, motivo) {
  var e = ESTADO[label];
  if (!e) {
    e = ESTADO[label] = { total: 0, ok: 0, enlaces: 0, off: false, caido: false, sin: false, motivo: "", audios: [] };
    ORDEN_ESTADO.push(label);
  }
  if (tipo === "off") { e.off = true; return; }
  if (tipo === "caido") { e.caido = true; return; }
  if (tipo === "sin") { e.sin = true; return; }
  e.total++;
  if (tipo === "ok") {
    e.ok++;
    e.enlaces += enlaces || 0;
    var a = abreviarAudio(audio);
    if (a && e.audios.indexOf(a) === -1) e.audios.push(a);
  } else if (!e.motivo) {
    e.motivo = motivo || "sin video en el embed";
  }
}
function abreviarAudio(a) {
  return { Latino: "LAT", Castellano: "CAS", Subtitulado: "SUB", "Espa\u00F1ol": "ESP" }[a] || "";
}
function lineaEstado(label) {
  var e = ESTADO[label];
  var audios = e.audios.length ? " [" + e.audios.join("/") + "]" : "";
  if (e.sin) return { r: 4, t: "\u2754 " + label + " \u2014 sin extractor" };
  if (e.caido) return { r: 4, t: "\u2754 " + label + " \u2014 servicio ca\u00EDdo (descartado)" };
  if (e.off) return { r: 3, t: "\u23F8\uFE0F " + label + " \u2014 desactivado" };
  if (e.ok === 0) return { r: 2, t: "\u274C " + label + " \u2014 " + e.motivo };
  if (e.ok < e.total) return { r: 1, t: "\u26A0\uFE0F " + label + " \u2014 " + e.ok + "/" + e.total + " embeds, " + e.enlaces + " enlaces" + audios };
  return { r: 0, t: "\u2705 " + label + " \u2014 " + e.enlaces + (e.enlaces === 1 ? " enlace" : " enlaces") + audios };
}
function hayFallos() {
  return ORDEN_ESTADO.some(function (l) { return !ESTADO[l].off && !ESTADO[l].sin && !ESTADO[l].caido && ESTADO[l].ok < ESTADO[l].total; });
}
function entradaEstado(titulo, conTrace) {
  var lineas = ORDEN_ESTADO.map(lineaEstado);
  lineas.sort(function (a, b) { return a.r - b.r; });
  var cuerpo = ["\uD83D\uDCE1 ESTADO DE REPRODUCTORES (no reproducir)", FUENTE + " v" + VERSION + (titulo ? " | " + titulo : "")]
    .concat(lineas.map(function (l) { return l.t; }));
  if (!lineas.length) cuerpo.push("(sin reproductores en esta consulta)");
  if (conTrace) cuerpo = cuerpo.concat(["\uD83D\uDEE0 DIAGNOSTICO"], TRACE);
  return { name: FUENTE, title: "", url: BASE + "/", quality: cuerpo.join("\n"), headers: {} };
}

// ---------- servidores ----------
function identificar(url) {
  var h = dominio(url);
  for (var i = 0; i < SERVIDORES.length; i++) if (SERVIDORES[i].host.test(h)) return SERVIDORES[i];
  return null;
}
async function resolverServidor(srv, url, referer) {
  var salida = await srv.run(url, referer);
  salida = (salida || []).filter(function (s) { return s && s.url; });
  for (var i = 0; i < salida.length; i++) {
    var s = salida[i];
    s.servidor = srv.label;
    if (!s.calidad) s.calidad = calidadUrlset(s.url);
    if (!s.calidad && restante() > 15000) s.calidad = await calidadHls(s.url, s.headers);
    if (!s.calidad) s.calidad = calidadTexto(s.url);
  }
  return salida;
}
function banderaAudio(a) {
  if (a === "Latino") return "\uD83C\uDDF2\uD83C\uDDFD LATINO";
  if (a === "Castellano") return "\uD83C\uDDEA\uD83C\uDDF8 CASTELLANO";
  if (a === "Espa\u00F1ol") return "\uD83C\uDF0E ESPA\u00D1OL";
  return "\uD83D\uDCAC SUBTITULADO";
}
async function procesarEmbed(e, tipoTxt) {
  var srv = identificar(e.url);
  if (!srv) { anotar(dominio(e.url).replace(/^www\./, "") || "?", "sin"); trace("sin extractor: " + dominio(e.url)); return []; }
  if (!ENABLED_SOURCES[srv.key]) { anotar(srv.label, "off"); return []; }
  if (srv.crypto && !CryptoJS) { anotar(srv.label, "fallo", e.audio, 0, "falta crypto-js"); return []; }
  var motivo = "";
  var tarea = (async function () {
    try { return await resolverServidor(srv, e.url, e.referer); } catch (err) { motivo = shortErr(err); return []; }
  })();
  var r = await conLimite(tarea, Math.max(2500, PRESUPUESTO - 4000 - transcurrido()), null);
  if (r === null) { motivo = "tiempo agotado"; r = []; }
  if (!r.length) {
    anotar(srv.label, "fallo", e.audio, 0, motivo);
    trace(srv.label + " (" + e.audio + ") fallo: " + (motivo || "sin video") + " | " + dominio(e.url));
    return [];
  }
  anotar(srv.label, "ok", e.audio, r.length);
  return r.map(function (v) {
    var o = {
      name: FUENTE,
      title: "",
      url: v.url,
      quality: "\uD83D\uDCFA " + srv.label + "\n" + (v.calidad || e.calidad || "Auto") + " | WEB-DL | " + tipoTxt + "\n" + banderaAudio(e.audio),
      headers: v.headers || {},
      _audio: e.audio,
      _rank: SERVER_ORDER.indexOf(srv.key),
      _q: pesoCalidad(v.calidad || e.calidad)
    };
    if (/\.m3u8/i.test(v.url)) o.type = "hls";
    return o;
  });
}

// ---------- punto de entrada ----------
async function getStreams(tmdbId, mediaType, season, episode) {
  TRACE = []; ESTADO = {}; ORDEN_ESTADO = [];
  trace(FUENTE + " v" + VERSION);
  var titulo = "";
  try {
    var tipo = mediaType === "movie" ? "movie" : "tv";
    var temporada = Number(season) || 1;
    var episodio = Number(episode) || 1;
    var datos = await datosTmdb(tmdbId, tipo);
    if (!datos) { trace("TMDB fallo"); return DEBUG ? [entradaEstado("", true)] : []; }
    titulo = encabezado(datos, tipo, temporada, episodio);
    trace("TMDB: " + titulo);
    var post = (await buscarCorto(datos, tipo)) || (await buscarPost(datos, tipo));
    if (!post) { trace("no se encontro en allcalidad"); return DEBUG ? [entradaEstado(titulo, true)] : []; }
    trace("post: " + post._id + " " + String(post.title || "").slice(0, 40));
    var id = post._id;
    if (tipo === "tv") {
      var eps = await json(API + "/episodes?post_id=" + post._id, { headers: CABECERAS });
      var ep = ((eps && eps.data) || []).find(function (x) { return Number(x.season_number) === temporada && Number(x.episode_number) === episodio; });
      if (!ep) { trace("episodio T" + temporada + "E" + episodio + " no existe (" + (((eps && eps.data) || []).length) + " episodios listados)"); return DEBUG ? [entradaEstado(titulo, true)] : []; }
      id = ep._id;
    }
    var r = await json(API + "/player?post_id=" + id + "&_any=1", { headers: CABECERAS });
    var embeds = ((r && r.data && r.data.embeds) || []).map(function (e) {
      return { url: String(e.url || "").replace(/\\\//g, "/"), lang: e.lang, quality: e.quality };
    }).filter(function (e) { return /^https?:/.test(e.url); });
    trace("player: " + embeds.length + " embeds" + (r ? "" : " (sin respuesta)"));
    var vistos = {};
    var lista = [];
    embeds.forEach(function (e) {
      if (vistos[e.url]) return;
      vistos[e.url] = true;
      if (DESCARTADOS.test(e.url)) { anotar(dominio(e.url).replace(/^www\./, ""), "caido"); return; }
      lista.push({ url: e.url, audio: audioDe(e.lang) || "Latino", calidad: calidadTexto(e.quality), referer: BASE + "/" });
    });
    var tipoTxt = tipo === "movie" ? "Pel\u00EDcula" : "Serie";
    var grupos = await Promise.all(lista.map(function (e) { return procesarEmbed(e, tipoTxt); }));
    var unicos = {};
    var results = [];
    [].concat.apply([], grupos).forEach(function (s) {
      if (unicos[s.url]) return;
      unicos[s.url] = true;
      results.push(s);
    });
    results.sort(function (a, b) {
      var pa = pesoAudio(a._audio), pb = pesoAudio(b._audio);
      if (pa !== pb) return pb - pa;
      if (a._rank !== b._rank) return a._rank - b._rank;
      return b._q - a._q;
    });
    results.forEach(function (s) { delete s._audio; delete s._rank; delete s._q; });
    trace(results.length + " streams");
    if (SHOW_STATUS && ORDEN_ESTADO.length) results.push(entradaEstado(titulo, DEBUG && (!results.length || hayFallos())));
    else if (!results.length && DEBUG) results.push(entradaEstado(titulo, true));
    return results;
  } catch (e) {
    trace("error: " + shortErr(e));
    return DEBUG ? [entradaEstado(titulo, true)] : [];
  }
}

exports.getStreams = limitar(getStreams);
