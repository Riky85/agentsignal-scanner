"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  AgentSignal — Digital Maturity Intelligence Engine v11                     ║
║  Paradigma: Technology & Business Intelligence                               ║
║                                                                              ║
║  COSA RILEVIAMO (alta affidabilità da CDN fingerprint):                      ║
║    E-Commerce  · Payments  · CRM  · Marketing  · Analytics                  ║
║    Support     · Automation · Cloud · Framework · Monitoring                ║
║                                                                              ║
║  AI: solo segnali pubblici (Careers, Blog, Product page, Docs)               ║
║      NON codice lato-server (irrilevabile con HTTP scraping)                 ║
║                                                                              ║
║  OUTPUT (6 score Digital Maturity):                                          ║
║    digital_maturity   · ai_readiness  · automation_level                    ║
║    cloud_maturity     · commerce_stack · buying_intent                      ║
║                                                                              ║
║  VALORE VENDUTO: "Find 50 companies/month most likely to buy your service"  ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import re
import json

# ══════════════════════════════════════════════════════════════════════════════
# BIZ STACK — CDN fingerprint inequivocabili
# Ogni pattern appare SOLO se il sito usa davvero il tool (client-side CDN)
# ══════════════════════════════════════════════════════════════════════════════

BIZ_CDN = {
    # ── E-COMMERCE ───────────────────────────────────────────────────────────
    "Shopify": [
        r"cdn\.shopify\.com/s/files/",
        r"\.myshopify\.com",
        r"Shopify\.theme\b",
        r'"shopify-section"',
        r"shopify\.analytics",
    ],
    "WooCommerce": [
        r"/wp-content/plugins/woocommerce/",
        r'"woocommerce-page"',
        r"wc-shortcodes",
    ],
    "Magento": [
        r'"Magento_(?!Media)',   # esclude Magento_MediaGallery falsi pos.
        r"requirejs/require\.js.*?mage",
        r"var\s+require\s*=\s*\{.*?baseUrl.*?mage",
    ],
    "BigCommerce": [
        r"cdn\d*\.bigcommerce\.com",
        r"stencil\.js",
        r"BigCommerce\s*\.",
    ],
    "PrestaShop": [
        r"/themes/classic/assets/css/",
        r"prestashop(?:-analytics)?\.js",
    ],
    "Squarespace": [
        r"static\d*\.squarespace\.com/static/",
        r"squarespace-cdn\.com",
    ],
    "Wix": [
        r"static\.wixstatic\.com/media/",
        r"wix-thunderbolt",
        r"siteassets\.parastorage\.com",
    ],
    "Ecwid": [
        r"app\.ecwid\.com/script\.js\?\d+",
    ],

    # ── PAYMENTS ─────────────────────────────────────────────────────────────
    "Stripe": [
        r"js\.stripe\.com/v\d/stripe\.js",
        r'Stripe\s*\(\s*["\']pk_(?:live|test)_',
        r"stripejs\.stripe\.com",
    ],
    "PayPal": [
        r"paypal\.com/sdk/js\?client-id=",
        r"paypalobjects\.com/api/checkout\.js",
        r"paypal\.Buttons\s*\(",
    ],
    "Adyen": [
        r"checkoutshopper-(?:live|test)\.adyen\.com",
        r"adyen\.com/hpp/cse/js/",
    ],
    "Braintree": [
        r"js\.braintreegateway\.com/web/",
        r"braintree-web/",
    ],
    "Klarna": [
        r"js\.klarna\.com/",
        r"klarna-payments",
        r"osm\.klarnaservices\.com",
    ],
    "Mollie": [
        r"js\.mollie\.com/v\d",
    ],
    "Paddle": [
        r"cdn\.paddle\.com/paddle/paddle\.js",
    ],
    "Chargebee": [
        r"js\.chargebee\.com/v2/chargebee\.js",
    ],
    "Recurly": [
        r"js\.recurly\.com/v\d/recurly\.js",
    ],

    # ── CRM ──────────────────────────────────────────────────────────────────
    "HubSpot": [
        r"js\.hs-scripts\.com/\d+\.js",
        r"js\.hsforms\.net/",
        r"hs-analytics\.net",
        r"hsappstatic\.net",
        r"\.hubspot\.com/hs/hsstatic/",
    ],
    "Salesforce": [
        r"salesforceliveagent\.com/content/g/js/",
        r"salesforce-chat",
        r"\.salesforce\.com/embeddedservice/",
    ],
    "Pipedrive": [
        r"pipedriveassets\.com",
    ],
    "Zoho": [
        r"salesiq\.zoho\.com/widget",
        r"zohopublic\.com/crm/",
    ],
    "ActiveCampaign": [
        r"trackcmp\.net/",
        r"activehosted\.com/f/",
    ],
    "Freshsales": [
        r"freshsales\.io/crm/",
    ],
    "Monday": [
        r"monday\.com/apps/",
        r"dapulse\.com/",
    ],
    "Copper": [
        r"prosperworks\.com/",
    ],

    # ── SUPPORT ──────────────────────────────────────────────────────────────
    "Intercom": [
        r"widget\.intercom\.io/widget/",
        r'"intercomSettings"\s*=',
        r"intercom-container",
        r"app\.intercom\.io/javascript/",
    ],
    "Zendesk": [
        r"static\.zdassets\.com/ekr/snippet\.js",
        r'ze\s*\(\s*"webWidget"',
        r"zopim\.com/s/",
    ],
    "Freshdesk": [
        r"fw-cdn\.com/fresh(?:desk|chat)\.js",
        r"wchat\.freshchat\.com/js/widget\.js",
    ],
    "Crisp": [
        r"client\.crisp\.chat/",
        r"CRISP_WEBSITE_ID\s*=",
    ],
    "Drift": [
        r"js\.driftt\.com/include/",
        r'"driftt_aim"',
    ],
    "Tidio": [
        r"code\.tidio\.co/",
    ],
    "LiveChat": [
        r"cdn\.livechatinc\.com/tracking\.js",
    ],
    "Tawk": [
        r"embed\.tawk\.to/",
        r"tawk\.to/s1/",
    ],

    # ── MARKETING ────────────────────────────────────────────────────────────
    "Mailchimp": [
        r"chimpstatic\.com/mcjs-connected",
        r"mailchimp\.com/connect/",
    ],
    "Klaviyo": [
        r"static\.klaviyo\.com/onsite/js/",
        r"a\.klaviyo\.com",
    ],
    "Brevo": [
        r"sibforms\.com/serve/",
        r"Sib_options\s*=",
    ],
    "Iterable": [
        r"js\.iterable\.com/",
    ],
    "Customer.io": [
        r"assets\.customer\.io/assets/track\.js",
    ],
    "Marketo": [
        r"munchkin\.marketo\.net/munchkin\.js",
        r"marketo\.com/index\.php/leadCapture/",
    ],

    # ── ANALYTICS ────────────────────────────────────────────────────────────
    "Google Analytics 4": [
        r"googletagmanager\.com/gtag/js\?id=G-",
        r'gtag\s*\(["\']config["\'],\s*["\']G-',
    ],
    "Google Tag Manager": [
        r"googletagmanager\.com/gtm\.js\?id=GTM-",
    ],
    "Mixpanel": [
        r"cdn4?\.mxpnl\.com/libs/",
    ],
    "Amplitude": [
        r"cdn\.amplitude\.com/libs/amplitude-\d",
        r"analytics\.amplitude\.com/",
    ],
    "Segment": [
        r"cdn\.segment\.com/analytics\.js/v\d",
        r"api\.segment\.io/v1/",
    ],
    "PostHog": [
        r"(?:app|eu)\.posthog\.com/static/array\.js",
        r"POSTHOG_HOST\s*=",
    ],
    "Heap": [
        r"heapanalytics\.com/js/heap-\d+\.js",
        r"heap\.load\s*\(",
    ],
    "FullStory": [
        r"fullstory\.com/s/fs\.js",
        r"FS\.identify\s*\(",
    ],
    "Hotjar": [
        r"static\.hotjar\.com/c/hotjar-\d+\.js",
        r"hj\s*=\s*window\.hj",
    ],
    "Plausible": [
        r"plausible\.io/js/(?:script|plausible)\.js",
    ],
    "Matomo": [
        r"matomo\.js",
        r"_paq\s*=\s*window\._paq",
    ],

    # ── MONITORING / DEVOPS ──────────────────────────────────────────────────
    "Sentry": [
        r"browser\.sentry-cdn\.com/\d",
        r"sentry\.io/api/\d+/envelope/",
    ],
    "Datadog": [
        r"datadoghq-browser-agent\.com/",
        r"browser-sdk\.datadoghq\.com/",
    ],
    "LogRocket": [
        r"cdn\.lr-in\.com/LogRocket\.min\.js",
    ],
    "Pendo": [
        r"cdn\.pendo\.io/agent/static/",
    ],
    "New Relic": [
        r"js-agent\.newrelic\.com/",
        r"newrelic\.agent\(",
    ],
    "LaunchDarkly": [
        r"app\.launchdarkly\.com/sdk/evalx/",
        r"unpkg\.com/launchdarkly-js-client-sdk",
    ],

    # ── AUTOMATION ───────────────────────────────────────────────────────────
    "n8n": [
        r"n8n-widget",
        r"app\.n8n\.io/embed",
    ],
    "Make": [
        r"integromat\.com",
        r"make\.com/oauth/api/embed",
    ],
    "Zapier": [
        r"zapier\.com/(?:partner|embed)/",
        r"cdn\.zapier\.com/",
    ],
    "Retool": [
        r"retool\.com/embedded/public/",
    ],
    "Bubble": [
        r"cdn\.bubble\.io/",
        r"bubble\.io/app/",
    ],
}

# Categoria per ogni tool
BIZ_CATEGORIES = {
    "ecommerce":   ["Shopify","WooCommerce","Magento","BigCommerce","PrestaShop","Squarespace","Wix","Ecwid"],
    "payments":    ["Stripe","PayPal","Adyen","Braintree","Klarna","Mollie","Paddle","Chargebee","Recurly"],
    "crm":         ["HubSpot","Salesforce","Pipedrive","Zoho","ActiveCampaign","Freshsales","Monday","Copper"],
    "support":     ["Intercom","Zendesk","Freshdesk","Crisp","Drift","Tidio","LiveChat","Tawk"],
    "marketing":   ["Mailchimp","Klaviyo","Brevo","Iterable","Customer.io","Marketo"],
    "analytics":   ["Google Analytics 4","Google Tag Manager","Mixpanel","Amplitude","Segment",
                    "PostHog","Heap","FullStory","Hotjar","Plausible","Matomo"],
    "monitoring":  ["Sentry","Datadog","LogRocket","Pendo","New Relic","LaunchDarkly"],
    "automation":  ["n8n","Make","Zapier","Retool","Bubble"],
}

# ══════════════════════════════════════════════════════════════════════════════
# TECH / FRAMEWORK fingerprint
# ══════════════════════════════════════════════════════════════════════════════

TECH_SIGNATURES = [
    # Framework frontend
    ("React",      [r"react\.production\.min\.js", r"/react@\d+\.\d",
                    r"__reactFiber[A-Za-z0-9]+", r'data-reactroot']),
    ("Next.js",    [r"/_next/static/chunks/", r"__NEXT_DATA__"]),
    ("Vue",        [r"vue\.global\.prod\.min\.js", r"/vue@\d+\.\d",
                    r"__vue_app__", r"data-v-app"]),
    ("Angular",    [r'ng-version="\d', r"/zone\.js@\d"]),
    ("Nuxt",       [r"__NUXT_DATA__", r"/_nuxt/builds/"]),
    ("Svelte",     [r"/svelte@\d+\.\d", r"__svelte[A-Za-z]"]),
    ("Remix",      [r"__remixContext", r"/build/root-[a-f0-9]+\.js"]),
    ("Gatsby",     [r"gatsby-chunk-mapping", r"/gatsby-browser"]),
    # Hosting / CDN (rilevabili da URL)
    ("Vercel",     [r"\.vercel\.app", r"/_vercel/insights/script\.js"]),
    ("Netlify",    [r"netlify-identity-widget\.js", r"netlify\.app"]),
    ("Cloudflare", [r"cloudflare\.com/cdn-cgi/", r"__cf_bm="]),
    ("AWS",        [r"\.s3\.amazonaws\.com/", r"\.cloudfront\.net/"]),
    ("GCP",        [r"\.storage\.googleapis\.com/", r"\.googlecloud\.com/"]),
    ("Azure",      [r"\.azurewebsites\.net", r"\.blob\.core\.windows\.net/"]),
    # Backend/BaaS
    ("Supabase",   [r"supabase\.co/rest/v1", r"supabase\.co/storage/"]),
    ("Firebase",   [r"firebaseapp\.com/__/firebase/init\.js",
                    r"firebase\.googleapis\.com/v\d"]),
    # CMS
    ("WordPress",  [r"/wp-content/themes/[a-zA-Z0-9\-_]+/",
                    r"/wp-includes/js/wp-embed\.", r"wp-json/wp/v2"]),
    ("Webflow",    [r"assets\.website-files\.com/[a-f0-9]{24}/",
                    r"uploads-ssl\.webflow\.com/"]),
    ("Contentful", [r"cdn\.contentful\.com", r"ctfassets\.net"]),
    ("Sanity",     [r"cdn\.sanity\.io/images/"]),
    ("Ghost",      [r"ghost\.io/assets/built/", r"/ghost/api/v\d"]),
    ("Framer",     [r"framerusercontent\.com/modules/", r"framer\.com/m/"]),
]

# ══════════════════════════════════════════════════════════════════════════════
# AI SIGNALS — solo da testo pubblico (NON da codice)
# Strategia: segnali di intenzione, prodotto, hiring
# ══════════════════════════════════════════════════════════════════════════════

AI_SIGNAL_PATTERNS = [
    # Hiring signals (careers page)
    {"type": "ai_hiring",    "weight": 15,
     "patterns": [
         r"\b(?:senior\s+)?(?:ai|ml|machine[\s-]learning|llm|nlp|genai)\s+engineer\b",
         r"\bhead\s+of\s+(?:ai|machine[\s-]learning|data[\s-]?science)\b",
         r"\bvp\s+of\s+ai\b",
         r"\bml\s+(?:ops|platform|infrastructure)\s+engineer\b",
         r"\bprompt\s+engineer\b",
         r"\bai\s+(?:product|safety|research)\s+(?:lead|manager|engineer)\b",
     ]},
    # Product signals (product/features page)
    {"type": "ai_product",   "weight": 20,
     "patterns": [
         r"\bai[\s-]?(?:copilot|assistant|search|chat|recommendations?|insights?)\b",
         r"\bpowered[\s-]by[\s-]ai\b",
         r"\b(?:smart|intelligent|predictive)\s+(?:search|recommendations?|automation|workflow)\b",
         r"\bgenerat(?:ive|ed)\s+ai\b",
         r"\bnatural[\s-]language\s+(?:processing|understanding|interface)\b",
     ]},
    # Blog / announcement signals
    {"type": "ai_blog",      "weight": 10,
     "patterns": [
         r"\blaunch(?:ed|ing)?\s+(?:our\s+)?ai\b",
         r"\bintroduc(?:ing|ed)\s+(?:ai|llm|chatbot|copilot)\b",
         r"\bbuilt\s+(?:on|with)\s+(?:openai|claude|gemini|llama|gpt)\b",
         r"\bai-(?:first|native|powered)\b",
     ]},
    # Documentation / API signals
    {"type": "ai_docs",      "weight": 12,
     "patterns": [
         r"\bai[\s-]?api\b",
         r"\bembeddings?\s+(?:api|endpoint|search)\b",
         r"\bsemantic\s+search\b",
         r"\bvector\s+(?:search|store|database|embeddings?)\b",
         r"\bllm[\s-]?(?:api|integration|gateway)\b",
     ]},
    # Release notes signals
    {"type": "ai_changelog",  "weight": 8,
     "patterns": [
         r"\badded\s+ai\s+(?:search|suggestions?|assistant|chat)\b",
         r"\bai[\s-]generated\s+(?:summaries|content|insights?)\b",
         r"\bnew\s*:\s*ai\b",
         r"\bai\s+(?:beta|preview|early[\s-]access)\b",
     ]},
]

# ══════════════════════════════════════════════════════════════════════════════
# DETECTION ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def detect_biz_stack(html: str, bundles: list) -> dict:
    """
    Rileva Business Stack solo da CDN fingerprint.
    Restituisce dict {categoria: [tool1, tool2, ...]}
    """
    # Corpus: HTML + primi 30KB di ogni bundle (max 4)
    corpus = html + "\n" + "\n".join(b[:30_000] for b in bundles[:4])

    by_category = {}
    for cat, tools in BIZ_CATEGORIES.items():
        hits = []
        for tool in tools:
            patterns = BIZ_CDN.get(tool, [])
            for pat in patterns:
                try:
                    if re.search(pat, corpus, re.IGNORECASE):
                        hits.append(tool)
                        break
                except re.error:
                    pass
        if hits:
            by_category[cat] = hits

    return by_category


def detect_tech_stack(html: str, bundles: list) -> list:
    """
    Rileva Framework/Cloud/CMS da fingerprint tecnici.
    Restituisce lista piatta [React, Next.js, AWS, ...]
    """
    corpus = html + "\n" + "\n".join(b[:30_000] for b in bundles[:4])
    found  = []
    for name, patterns in TECH_SIGNATURES:
        for pat in patterns:
            try:
                if re.search(pat, corpus, re.IGNORECASE) and name not in found:
                    found.append(name)
                    break
            except re.error:
                pass
    return found


def detect_ai_signals(pages: dict) -> list:
    """
    Rileva segnali AI da testo di pagine pubbliche.
    pages = {"careers": str, "blog": str, "product": str, "docs": str, "changelog": str}
    Restituisce lista di {type, signal_text, weight}
    """
    # Mapping pagina → tipi di segnale rilevanti
    PAGE_SIGNAL_MAP = {
        "careers":   ["ai_hiring"],
        "blog":      ["ai_blog", "ai_product"],
        "product":   ["ai_product"],
        "features":  ["ai_product"],
        "docs":      ["ai_docs"],
        "changelog": ["ai_changelog"],
        "homepage":  ["ai_product"],
    }

    signals = []
    seen    = set()

    for page_name, text in pages.items():
        if not text:
            continue
        text_lower = text.lower()
        relevant_types = PAGE_SIGNAL_MAP.get(page_name, [])

        for sig_def in AI_SIGNAL_PATTERNS:
            if sig_def["type"] not in relevant_types:
                continue
            for pat in sig_def["patterns"]:
                try:
                    m = re.search(pat, text_lower)
                    if m:
                        snippet = text_lower[max(0, m.start()-30):m.end()+60].strip()
                        key     = (sig_def["type"], m.group(0)[:40])
                        if key not in seen:
                            seen.add(key)
                            signals.append({
                                "type":   sig_def["type"],
                                "signal": snippet[:120],
                                "page":   page_name,
                                "weight": sig_def["weight"],
                            })
                except re.error:
                    pass

    return signals


# ══════════════════════════════════════════════════════════════════════════════
# DIGITAL MATURITY SCORING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def calc_digital_maturity_scores(
    biz_stack:  dict,   # {cat: [tools]}
    tech_stack: list,   # [React, AWS, ...]
    ai_signals: list,   # [{type, weight, ...}]
    employee_count: int = 0,
) -> dict:
    """
    Calcola 6 score Digital Maturity Intelligence (0-100).

    digital_maturity  — quanto è avanzata digitalmente
    ai_readiness      — quanto è pronta ad adottare AI (segnali pubblici)
    automation_level  — quanto ha già automatizzato
    cloud_maturity    — maturità infrastruttura cloud
    commerce_stack    — solidità stack commerciale
    buying_intent     — probabilità di acquistare servizi tech B2B
    """

    def c(v): return min(100, max(0, int(v)))

    # Stack counts
    ecom   = biz_stack.get("ecommerce", [])
    pay    = biz_stack.get("payments", [])
    crm    = biz_stack.get("crm", [])
    supp   = biz_stack.get("support", [])
    mkt    = biz_stack.get("marketing", [])
    anal   = biz_stack.get("analytics", [])
    mon    = biz_stack.get("monitoring", [])
    auto   = biz_stack.get("automation", [])

    cloud_tools = [t for t in tech_stack if t in {"AWS","GCP","Azure","Cloudflare","Vercel","Netlify","Firebase","Supabase"}]
    dev_tools   = [t for t in tech_stack if t in {"React","Next.js","Vue","Angular","Nuxt","Svelte","Remix","Gatsby"}]
    cms_tools   = [t for t in tech_stack if t in {"WordPress","Webflow","Contentful","Sanity","Ghost","Framer"}]

    total_tools  = len(set(t for tools in biz_stack.values() for t in tools)) + len(tech_stack)
    ai_weight    = sum(s["weight"] for s in ai_signals)
    ai_hire      = sum(1 for s in ai_signals if s["type"] == "ai_hiring")
    ai_prod      = sum(1 for s in ai_signals if s["type"] == "ai_product")

    # ── Digital Maturity (0-100) ──────────────────────────────────────────────
    # Quante categorie ha coperto? Più tools = più matura.
    cat_coverage   = len([c for c in [ecom,pay,crm,supp,mkt,anal,mon,auto] if c])  # 0-8
    tool_diversity = min(total_tools * 3, 40)
    analytics_pts  = min(len(anal) * 8, 20)    # analytics avanzate = maturità
    monitoring_pts = min(len(mon) * 8, 15)     # monitoring = engineering culture
    dev_pts        = min(len(dev_tools) * 5, 15)
    digital_maturity = c(cat_coverage * 5 + tool_diversity + analytics_pts + monitoring_pts + dev_pts)

    # ── AI Readiness (0-100) ──────────────────────────────────────────────────
    # Basato SOLO su segnali pubblici: hiring, product, blog, docs
    ai_readiness = c(
        min(ai_weight, 50)          # cap a 50 dai segnali
        + min(ai_hire * 12, 30)     # hiring = investimento strutturale
        + min(ai_prod * 15, 30)     # product AI = già in produzione
        + (5 if any(s["type"]=="ai_docs" for s in ai_signals) else 0)
    )

    # ── Automation Level (0-100) ──────────────────────────────────────────────
    # Quante automazioni/tool integration ha già?
    automation_level = c(
        len(auto) * 25              # n8n/Make/Zapier
        + min(len(crm) * 8, 25)    # CRM = processi strutturati
        + min(len(mkt) * 6, 20)    # marketing automation
        + (10 if "Segment" in anal or "PostHog" in anal else 0)  # event tracking
    )

    # ── Cloud Maturity (0-100) ────────────────────────────────────────────────
    cloud_maturity = c(
        min(len(cloud_tools) * 20, 60)
        + min(len(mon) * 10, 25)    # monitoring = cloud-native ops
        + (15 if any(t in tech_stack for t in ["Vercel","Netlify"]) else 0)
        + (10 if any(t in tech_stack for t in ["Supabase","Firebase"]) else 0)
    )

    # ── Commerce Stack (0-100) ────────────────────────────────────────────────
    # Ha un e-commerce funzionale e maturo?
    commerce_stack = c(
        len(ecom) * 25             # piattaforma e-commerce
        + len(pay) * 15            # gateway di pagamento
        + min(len(anal) * 5, 15)  # analytics di supporto
        + (10 if crm else 0)       # CRM collegato
        + (5 if mkt else 0)        # marketing automation
    )

    # ── Buying Intent (0-100) ─────────────────────────────────────────────────
    # Quanto è probabile che acquisti servizi tech B2B?
    # Alta se: molti tool + gap visibile (es. niente automazione ma CRM+payment)
    stack_richness   = min(total_tools * 4, 40)
    gap_automation   = 20 if (crm or pay or ecom) and not auto else 0  # usa CRM ma non ha automazione
    gap_ai           = 15 if (pay or crm or ecom) and not ai_signals else 0  # stack ricco ma no AI
    gap_crm          = 10 if (ecom or pay) and not crm else 0  # e-commerce senza CRM
    growth_signal    = min(ai_hire * 10, 20) + min(len(crm) * 5, 10)

    buying_intent = c(stack_richness + gap_automation + gap_ai + gap_crm + growth_signal)

    return {
        "digital_maturity":  digital_maturity,
        "ai_readiness":      ai_readiness,
        "automation_level":  automation_level,
        "cloud_maturity":    cloud_maturity,
        "commerce_stack":    commerce_stack,
        "buying_intent":     buying_intent,
        # Gap analysis (per service recommendation)
        "_gaps": {
            "no_automation":    bool(gap_automation),
            "no_ai_signal":     bool(gap_ai),
            "no_crm":           bool(gap_crm),
            "no_analytics":     not bool(anal),
            "no_monitoring":    not bool(mon),
            "no_support_tool":  not bool(supp),
        }
    }


def build_tech_dna(biz_stack: dict, tech_stack: list) -> dict:
    """
    Costruisce il Technology DNA dell'azienda.
    Struttura per categoria, con tutti i tool rilevati.
    """
    dna = {}
    for cat, tools in biz_stack.items():
        if tools:
            dna[cat] = tools
    if tech_stack:
        # Separa cloud, framework, CMS
        cloud = [t for t in tech_stack if t in {"AWS","GCP","Azure","Cloudflare","Vercel","Netlify","Firebase","Supabase"}]
        fw    = [t for t in tech_stack if t in {"React","Next.js","Vue","Angular","Nuxt","Svelte","Remix","Gatsby"}]
        cms   = [t for t in tech_stack if t in {"WordPress","Webflow","Contentful","Sanity","Ghost","Framer"}]
        other = [t for t in tech_stack if t not in cloud+fw+cms]
        if cloud: dna["cloud"]     = cloud
        if fw:    dna["framework"] = fw
        if cms:   dna["cms"]       = cms
        if other: dna["infra"]     = other
    return dna


def build_flat_tech_list(biz_stack: dict, tech_stack: list) -> list:
    """Lista piatta di tutti i tool rilevati — per Base44 tech_stack field."""
    tools = []
    for cat_tools in biz_stack.values():
        tools.extend(cat_tools)
    tools.extend(tech_stack)
    return list(dict.fromkeys(tools))  # dedup mantenendo ordine


def build_ai_signals_list(ai_signals: list) -> list:
    """Lista di stringhe leggibili dei segnali AI — per Base44 ai_stack field."""
    seen  = set()
    result = []
    for s in sorted(ai_signals, key=lambda x: -x["weight"]):
        label = f"[{s['type']}] {s['signal'][:80]}"
        if label not in seen:
            seen.add(label)
            result.append(label)
    return result[:10]  # top 10 segnali


def build_dna_summary(biz_stack: dict, tech_stack: list, ai_signals: list, scores: dict) -> str:
    """
    Testo leggibile per ats_documentation — il report Digital Maturity.
    """
    lines = ["=== Digital Maturity Intelligence Report ==="]

    # Tech stack per categoria
    for cat, tools in biz_stack.items():
        if tools:
            lines.append(f"{cat.title()}: {', '.join(tools)}")

    dna = build_tech_dna(biz_stack, tech_stack)
    for cat in ["cloud","framework","cms","infra"]:
        if dna.get(cat):
            lines.append(f"{cat.title()}: {', '.join(dna[cat])}")

    # AI signals
    if ai_signals:
        lines.append(f"AI Signals ({len(ai_signals)}): " +
                     ", ".join(s["type"] for s in ai_signals[:5]))

    # Scores
    lines.append(f"Digital Maturity={scores['digital_maturity']} "
                 f"AI Readiness={scores['ai_readiness']} "
                 f"Buying Intent={scores['buying_intent']}")

    # Gaps
    gaps = scores.get("_gaps", {})
    gap_labels = [k.replace("_"," ").title() for k, v in gaps.items() if v]
    if gap_labels:
        lines.append(f"Gaps: {', '.join(gap_labels)}")

    return " | ".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# SCORE MAPPING → campi Base44
# ══════════════════════════════════════════════════════════════════════════════

def map_scores_to_base44(scores: dict) -> dict:
    """
    Mappa i 6 score DMI sui campi Base44 esistenti (type=integer).
    """
    return {
        "ai_adoption_score":      scores["ai_readiness"],          # AI Readiness
        "ai_maturity_score":      min(5, scores["digital_maturity"] // 20),  # 0-5
        "ai_buying_intent_score": scores["buying_intent"],         # Buying Intent
        "ai_transformation_score":scores["digital_maturity"],      # Digital Maturity
        "cloud_score":            scores["cloud_maturity"],
        "automation_score":       scores["automation_level"],
        "commerce_score":         scores["commerce_stack"],
        "developer_score":        scores.get("_framework_pts", 0),
        "growth_score":           scores["buying_intent"],
        "innovation_score":       scores["ai_readiness"],
        "tech_gap_score":         min(100, sum(1 for v in scores.get("_gaps",{}).values() if v) * 15),
    }
