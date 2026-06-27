#!/usr/bin/env python3
"""
AgentSignal Detection Engine v10 — Technology & Business Intelligence
Paradigma: Digital Maturity Intelligence, non AI Stack Detection.

COSA RILEVA (con alta affidabilità da CDN/script tags):
  E-commerce, CMS, Analytics, CRM, Payments, Marketing, Cloud, Framework,
  Automation, Support, Security, DevOps

COME RILEVA AI:
  NON dal codice lato server (impossibile).
  DA SEGNALI PUBBLICI: careers, blog, product pages, release notes, docs.

OUTPUT:
  - tech_dna: dizionario per categoria
  - ai_signals: lista segnali AI (careers/product/blog/docs)
  - scores: Digital Maturity, AI Readiness, Automation Level, Commerce Stack, Buying Intent
"""

import re

# ══════════════════════════════════════════════════════════════════════════════
# TECHNOLOGY DNA — rilevazione da CDN/script/DOM (alta affidabilità ⭐⭐⭐⭐⭐)
# Ogni tool ha pattern che si trovano SOLO se il sito usa davvero quel tool.
# ══════════════════════════════════════════════════════════════════════════════

TECH_DNA = {

    "ecommerce": {
        "Shopify":      [r"cdn\.shopify\.com", r"shopify\.com/s/files", r"Shopify\.theme"],
        "WooCommerce":  [r"woocommerce", r"/wp-content/plugins/woo"],
        "Magento":      [r"Magento_Ui", r"mage/utils", r"Magento\."],
        "BigCommerce":  [r"bigcommerce\.com", r"cdn\d+\.bigcommerce\.com"],
        "Squarespace":  [r"squarespace\.com", r"static\.squarespace\.com"],
        "Wix":          [r"static\.wixstatic\.com", r"wix\.com/thunder"],
        "PrestaShop":   [r"prestashop", r"/modules/prestashop"],
        "Stripe Checkout": [r"js\.stripe\.com/v3/", r"checkout\.stripe\.com"],
    },

    "cms": {
        "WordPress":    [r"/wp-content/", r"/wp-includes/", r"wp-json"],
        "Webflow":      [r"webflow\.com/css", r"assets\.website-files\.com", r"uploads-ssl\.webflow\.com"],
        "Drupal":       [r"Drupal\.settings", r"/sites/default/files/", r"drupal\.js"],
        "Ghost":        [r"ghost\.io", r"/ghost/api/"],
        "Contentful":   [r"cdn\.contentful\.com", r"images\.ctfassets\.net"],
        "Sanity":       [r"cdn\.sanity\.io"],
        "Framer":       [r"framer\.com/m/", r"framerusercontent\.com"],
        "HubSpot CMS":  [r"hs-scripts\.com", r"hubspot\.net/cta/"],
    },

    "analytics": {
        "Google Analytics 4": [r"gtag\('config',\s*'G-", r"googletagmanager\.com/gtag"],
        "Segment":      [r"cdn\.segment\.com/analytics\.js", r"analytics\.load\("],
        "Mixpanel":     [r"cdn\.mxpnl\.com", r"mixpanel\.init\("],
        "Amplitude":    [r"cdn\.amplitude\.com", r"amplitude\.getInstance\("],
        "Hotjar":       [r"static\.hotjar\.com", r"hj\('trigger'"],
        "PostHog":      [r"app\.posthog\.com", r"posthog\.identify\("],
        "Heap":         [r"cdn\.heapanalytics\.com", r"heap\.load\("],
        "FullStory":    [r"rs\.fullstory\.com", r"fullstory\.com/s/fs\.js"],
        "Datadog RUM":  [r"browser-intake-datadoghq\.com", r"DD_RUM\.init\("],
        "Sentry":       [r"browser\.sentry-cdn\.com", r"sentry\.init\("],
    },

    "crm_marketing": {
        "HubSpot":      [r"js\.hs-scripts\.com/\d+\.js", r"hubspot\.com/conversations"],
        "Salesforce":   [r"salesforceliveagent\.com", r"force\.com/lightning"],
        "Intercom":     [r"widget\.intercom\.io", r"api\.intercom\.io", r"intercomSettings"],
        "Zendesk":      [r"static\.zdassets\.com", r"zopim\.com", r"zendesk\.com/embeddable"],
        "Drift":        [r"js\.driftt\.com", r"drift\.com/include\.js"],
        "Crisp":        [r"client\.crisp\.chat", r"CRISP_WEBSITE_ID"],
        "Freshdesk":    [r"freshdesk\.com/widget", r"freddy\.freshdesk\.com"],
        "Klaviyo":      [r"static\.klaviyo\.com", r"klaviyo\.init\("],
        "Mailchimp":    [r"chimpstatic\.com", r"list-manage\.com/track"],
        "ActiveCampaign":[r"trackcmp\.net", r"activecampaign\.com"],
        "Brevo":        [r"sibautomation\.com", r"sendinblue\.com/tracker"],
        "Pipedrive":    [r"pipedrive\.com/leadbooster", r"leadbooster\.pipedrive\.com"],
    },

    "payments": {
        "Stripe":       [r"js\.stripe\.com/v\d", r"stripe\.createToken"],
        "PayPal":       [r"paypal\.com/sdk/js", r"paypalobjects\.com"],
        "Adyen":        [r"checkoutshopper-live\.adyen\.com", r"adyen\.encrypt"],
        "Braintree":    [r"js\.braintreegateway\.com", r"braintree\.setup\("],
        "Square":       [r"js\.squareup\.com", r"squareupsandbox\.com"],
        "Recurly":      [r"js\.recurly\.com"],
        "Paddle":       [r"cdn\.paddle\.com/paddle/paddle\.js"],
        "Chargebee":    [r"js\.chargebee\.com"],
    },

    "cloud_infra": {
        "AWS CloudFront":[r"cloudfront\.net/", r"\.execute-api\..*\.amazonaws\.com"],
        "Cloudflare":   [r"cloudflareinsights\.com", r"cdn-cgi/challenge-platform", r"__cf_bm"],
        "Fastly":       [r"fastly\.net/", r"x-fastly-request-id"],
        "Vercel":       [r"vercel\.app", r"_vercel\.app", r"x-vercel-id"],
        "Netlify":      [r"netlify\.app", r"netlify\.com/build-status"],
        "Firebase":     [r"firebaseapp\.com", r"firebase\.google\.com"],
        "Supabase":     [r"supabase\.co/storage", r"supabase\.io"],
        "Azure CDN":    [r"azureedge\.net", r"azurewebsites\.net"],
        "Google Cloud": [r"storage\.googleapis\.com", r"\.run\.app"],
    },

    "framework_dev": {
        "React":        [r"react\.development\.js", r"__reactFiber", r"data-reactroot"],
        "Next.js":      [r"/_next/static/", r"__NEXT_DATA__", r"next/dist"],
        "Vue.js":       [r"vue\.min\.js", r"__vue__", r"v-bind:"],
        "Angular":      [r"angular\.min\.js", r"ng-version=", r"__ng_app_id__"],
        "Svelte":       [r"svelte-", r"__svelte"],
        "Nuxt.js":      [r"/_nuxt/", r"__NUXT__"],
        "Remix":        [r"__remix_router__", r"@remix-run"],
        "Astro":        [r"astro-island", r"astro/dist"],
        "GraphQL":      [r"__schema.*queryType", r"/graphql.*introspection"],
    },

    "automation_nocode": {
        "Zapier":       [r"zapier\.com/app/embed", r"hooks\.zapier\.com"],
        "Make":         [r"make\.com/webhook", r"integromat\.com"],
        "n8n":          [r"n8n\.io/webhook", r"n8n-webhook"],
        "Workato":      [r"workato\.com/webhooks"],
        "Tray.io":      [r"tray\.io/embed"],
        "Retool":       [r"retool\.com", r"tryretool\.com"],
        "Bubble":       [r"bubble\.io", r"bblbrx\.com"],
        "Webflow Logic":[r"webflow\.com/logic"],
    },

    "devops_monitoring": {
        "Datadog":      [r"datadoghq\.com/datadog\.js", r"DD_RUM"],
        "New Relic":    [r"nr-data\.net", r"newrelic\.com/browser-agent"],
        "LogRocket":    [r"cdn\.logrocket\.com", r"logrocket\.init\("],
        "Segment":      [r"cdn\.segment\.com"],
        "LaunchDarkly": [r"app\.launchdarkly\.com", r"ldclient\.min\.js"],
        "Pendo":        [r"cdn\.pendo\.io", r"pendo\.initialize\("],
        "Gainsight":    [r"web-sdk\.aptrinsic\.com"],
        "GitHub":       [r"github\.githubassets\.com"],
    },

    "security": {
        "Auth0":        [r"cdn\.auth0\.com", r"auth0\.js"],
        "Okta":         [r"okta\.com/auth/", r"okta-signin-widget"],
        "Cloudflare WAF":[r"challenges\.cloudflare\.com", r"__cflb"],
        "reCAPTCHA":    [r"google\.com/recaptcha", r"grecaptcha\.execute"],
        "hCaptcha":     [r"hcaptcha\.com/captcha"],
        "Clerk":        [r"clerk\.dev", r"clerk\.browser\.js"],
        "Supertokens":  [r"supertokens\.io"],
    },
}

# ══════════════════════════════════════════════════════════════════════════════
# AI SIGNALS — da pagine pubbliche (careers, blog, product, docs)
# Non rileva il modello specifico — rileva INTENZIONE e INVESTIMENTO AI
# ══════════════════════════════════════════════════════════════════════════════

AI_SIGNAL_PATTERNS = {

    "ai_hiring": {
        # Pagine careers/jobs
        "pages": ["/careers", "/jobs", "/hiring", "/about/jobs"],
        "patterns": [
            (r"\b(AI|ML|Machine Learning)\s+Engineer", "Senior AI/ML Engineer role"),
            (r"(Head|VP|Director)\s+of\s+(AI|Machine Learning)", "AI leadership hire"),
            (r"\bLLM\b|\bGenAI\b|\bGenerative\s+AI", "GenAI specialist search"),
            (r"\bMLOps\b|\bAI\s+Platform", "MLOps/AI Platform hire"),
            (r"\bPrompt\s+Engineer", "Prompt Engineer role"),
            (r"\bData\s+Scientist.*AI\b|\bAI.*Data\s+Scientist", "AI-focused Data Scientist"),
        ],
    },

    "ai_product": {
        # Pagine prodotto/features
        "pages": ["/features", "/product", "/solutions", "/platform", "/"],
        "patterns": [
            (r"\bAI\s+(Copilot|Assistant|Agent|Companion|Autopilot)\b", "AI assistant in product"),
            (r"\bPowered\s+by\s+(AI|Machine Learning)\b", "AI-powered claim"),
            (r"\b(Smart|Intelligent)\s+(Search|Suggest|Recommend)", "AI search/recommend feature"),
            (r"\bAI[-\s]?(generated|driven|enabled|native)\b", "AI-native feature"),
            (r"\bNatural\s+Language\b|\bChat\s+with\s+your\b", "NLP interface"),
            (r"\bAutomated?\s+(insight|report|analysis)\b", "Automated insights"),
        ],
    },

    "ai_blog": {
        # Ultime notizie/blog
        "pages": ["/blog", "/news", "/updates", "/changelog"],
        "patterns": [
            (r"(launched?|announc|introduc|released?)\s+.*\bAI\b", "AI launch announcement"),
            (r"\bAI\s+(feature|tool|integration|update)\b.*\d{4}", "Recent AI feature"),
            (r"\bGenerative\s+AI\b|\bLarge\s+Language\s+Model\b", "LLM/GenAI mention"),
            (r"\bGPT-\d|\bClaude\b|\bGemini\b|\bLlama\b", "Specific LLM mentioned"),
        ],
    },

    "ai_docs": {
        # Documentazione
        "pages": ["/docs", "/documentation", "/developers", "/api", "/help"],
        "patterns": [
            (r"\bAI\s+(endpoint|API|integration|model)\b", "AI API documented"),
            (r"\b/ai/\b|\b/ml/\b|\b/nlp/\b", "AI endpoint in docs"),
            (r"\bembedding|vector\s+search|semantic\s+search", "Vector/semantic search"),
        ],
    },
}

# ══════════════════════════════════════════════════════════════════════════════
# SCORING ENGINE — Digital Maturity Intelligence
# ══════════════════════════════════════════════════════════════════════════════

def compute_digital_maturity(tech_dna: dict, ai_signals: list) -> dict:
    """
    Calcola i 6 score proprietari di Digital Maturity Intelligence.
    Input: tech_dna (dizionario categoria→[tools]), ai_signals (lista segnali)
    Output: dizionario score 0-100
    """

    def has(category, *tools):
        cat = tech_dna.get(category, [])
        if not tools: return len(cat) > 0
        return any(t in cat for t in tools)

    def count(category):
        return len(tech_dna.get(category, []))

    def clamp(v): return max(0, min(100, int(v)))

    all_tools = sum(len(v) for v in tech_dna.values())

    # ── 1. Digital Maturity (0-100) ──────────────────────────────────────────
    # Quante categorie presidia l'azienda digitalmente
    dm = 0
    dm += min(25, count("analytics") * 8)           # Analytics sofisticata
    dm += min(20, count("framework_dev") * 7)        # Stack dev moderno
    dm += min(15, count("cloud_infra") * 5)          # Cloud maturity
    dm += min(15, count("devops_monitoring") * 6)    # Monitoring/DevOps
    dm += 10 if has("security") else 0               # Security awareness
    dm += 10 if has("crm_marketing", "HubSpot", "Salesforce", "Pipedrive") else 0  # CRM
    dm += 5  if all_tools >= 10 else (3 if all_tools >= 5 else 0)

    # ── 2. AI Readiness (0-100) ──────────────────────────────────────────────
    # Non se usa AI — se è PRONTA per adottare AI
    ar = 0
    ai_signal_count = len(ai_signals)
    ar += min(40, ai_signal_count * 8)               # Segnali AI diretti
    ar += 15 if has("framework_dev", "Next.js", "React") else 0  # Stack moderno
    ar += 10 if has("cloud_infra") else 0            # Cloud (prerequisito AI)
    ar += 10 if has("analytics", "Segment", "Amplitude", "PostHog") else 0  # Data maturity
    ar += 10 if has("devops_monitoring") else 0      # DevOps = adozione tech veloce
    ar += 10 if has("automation_nocode") else 0      # Già fa automazione
    ar += 5  if dm >= 60 else 0                      # Maturità digitale alta

    # ── 3. Automation Level (0-100) ──────────────────────────────────────────
    # Quanto è già automatizzata
    al = 0
    al += min(40, count("automation_nocode") * 15)   # Tool no-code
    al += 20 if has("crm_marketing") else 0          # Marketing automation
    al += 15 if has("analytics", "Segment") else 0  # Event-driven analytics
    al += 15 if has("devops_monitoring", "LaunchDarkly", "Pendo") else 0
    al += 10 if has("payments") else 0               # Pagamenti automatizzati

    # ── 4. Commerce Stack (0-100) ────────────────────────────────────────────
    cs = 0
    cs += min(40, count("ecommerce") * 15)
    cs += min(20, count("payments") * 8)
    cs += 20 if has("crm_marketing", "Klaviyo", "Mailchimp") else 0
    cs += 10 if has("analytics") else 0
    cs += 10 if has("cms") else 0

    # ── 5. Cloud Maturity (0-100) ────────────────────────────────────────────
    cm = 0
    cm += min(40, count("cloud_infra") * 12)
    cm += min(20, count("devops_monitoring") * 7)
    cm += 15 if has("framework_dev", "Next.js", "Remix", "Astro") else 0
    cm += 15 if has("security", "Auth0", "Okta", "Clerk") else 0
    cm += 10 if has("cloud_infra", "Vercel", "Netlify") else 0

    # ── 6. Buying Intent (0-100) ─────────────────────────────────────────────
    # Probabilità che un venditore tech trovi terreno fertile
    bi = 0
    bi += 20 if dm >= 70 else (12 if dm >= 50 else 5)
    bi += 20 if ar >= 60 else (12 if ar >= 35 else 0)
    bi += 15 if al >= 50 else (8 if al >= 25 else 0)
    bi += 15 if cs >= 50 else (8 if cs >= 25 else 0)
    bi += 15 if cm >= 50 else (8 if cm >= 25 else 0)
    bi += min(15, ai_signal_count * 4)

    return {
        "digital_maturity":   clamp(dm),
        "ai_readiness":       clamp(ar),
        "automation_level":   clamp(al),
        "commerce_stack":     clamp(cs),
        "cloud_maturity":     clamp(cm),
        "buying_intent":      clamp(bi),
        # Legacy fields per compatibilità
        "ai_adoption_score":  clamp(ar),
        "ai_maturity_score":  clamp(dm / 20),  # 0-5
        "ai_buying_intent_score": clamp(bi),
        "automation_score":   clamp(al),
        "cloud_score":        clamp(cm),
        "commerce_score":     clamp(cs),
        "growth_score":       clamp((dm + ar) / 2),
        "innovation_score":   clamp((ar + cm) / 2),
        "developer_score":    clamp((count("framework_dev") * 15 + count("devops_monitoring") * 10)),
        "security_score":     clamp(count("security") * 25),
    }


def detect_tech_dna(html: str, bundles: list) -> dict:
    """
    Rileva il Technology DNA dell'azienda da HTML e bundle JS.
    Alta affidabilità: solo pattern CDN/signature univoci.
    """
    corpus = html + " " + " ".join(b[:50000] for b in bundles[:5])
    result = {}
    for category, tools in TECH_DNA.items():
        found = []
        for tool_name, patterns in tools.items():
            for pat in patterns:
                try:
                    if re.search(pat, corpus, re.IGNORECASE):
                        if tool_name not in found:
                            found.append(tool_name)
                        break
                except re.error:
                    pass
        if found:
            result[category] = found
    return result


def detect_ai_signals(pages: dict) -> list:
    """
    Rileva segnali AI da pagine pubbliche (careers, blog, product, docs).
    pages = {"careers": html_str, "blog": html_str, ...}
    Ritorna lista di segnali: [{"type": "ai_hiring", "signal": "...", "page": "..."}]
    """
    signals = []
    for signal_type, config in AI_SIGNAL_PATTERNS.items():
        for page_key, html in pages.items():
            if not html: continue
            for pattern, description in config["patterns"]:
                try:
                    if re.search(pattern, html, re.IGNORECASE):
                        signals.append({
                            "type":    signal_type,
                            "signal":  description,
                            "page":    page_key,
                        })
                except re.error:
                    pass
    # Deduplica per description
    seen = set()
    deduped = []
    for s in signals:
        k = s["signal"]
        if k not in seen:
            seen.add(k)
            deduped.append(s)
    return deduped


def build_technology_dna_summary(tech_dna: dict, ai_signals: list) -> str:
    """
    Genera un sommario testuale del Technology DNA per il campo ats_documentation.
    """
    lines = ["Technology & Business Intelligence Report"]
    lines.append("=" * 45)

    cat_labels = {
        "ecommerce":          "E-Commerce",
        "cms":                "CMS",
        "analytics":          "Analytics",
        "crm_marketing":      "CRM & Marketing",
        "payments":           "Payments",
        "cloud_infra":        "Cloud & Infra",
        "framework_dev":      "Framework & Dev",
        "automation_nocode":  "Automation",
        "devops_monitoring":  "DevOps & Monitoring",
        "security":           "Security",
    }

    for cat, label in cat_labels.items():
        tools = tech_dna.get(cat, [])
        if tools:
            lines.append(f"{label}: {', '.join(tools)}")

    if ai_signals:
        lines.append("")
        lines.append("AI Signals:")
        for s in ai_signals[:6]:
            lines.append(f"  [{s['type'].replace('ai_','')}] {s['signal']}")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# TEST RAPIDO
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    test_html = """
    <script src="https://cdn.shopify.com/s/files/1/theme.js"></script>
    <script src="https://js.stripe.com/v3/"></script>
    <script src="https://js.hs-scripts.com/12345.js"></script>
    <script src="https://cdn.segment.com/analytics.js/v1/write_key/analytics.min.js"></script>
    <script src="https://static.hotjar.com/c/hotjar.js"></script>
    <meta name="generator" content="next.js">
    <script>window.__NEXT_DATA__ = {}</script>
    <script src="https://challenges.cloudflare.com/turnstile.js"></script>
    <script>window.CRISP_WEBSITE_ID = "abc123"</script>
    """

    careers_html = """
    We're hiring a Senior AI Engineer to join our ML platform team.
    Looking for LLM experience and GenAI deployment skills.
    Director of AI to lead our new AI Copilot initiative.
    """

    product_html = """
    Powered by AI. Our AI Copilot helps you automate workflows.
    Smart Search powered by Machine Learning.
    """

    tech_dna    = detect_tech_dna(test_html, [])
    ai_signals  = detect_ai_signals({"careers": careers_html, "product": product_html})
    scores      = compute_digital_maturity(tech_dna, ai_signals)
    summary     = build_technology_dna_summary(tech_dna, ai_signals)

    print("=== TECH DNA ===")
    for cat, tools in tech_dna.items():
        print(f"  {cat:20s}: {', '.join(tools)}")

    print("\n=== AI SIGNALS ===")
    for s in ai_signals:
        print(f"  [{s['type']}] {s['signal']} (from /{s['page']})")

    print("\n=== DIGITAL MATURITY SCORES ===")
    key_scores = ["digital_maturity","ai_readiness","automation_level","commerce_stack","cloud_maturity","buying_intent"]
    for k in key_scores:
        bar = "█" * (scores[k]//10) + "░" * (10 - scores[k]//10)
        print(f"  {k:20s}: {scores[k]:3d}  {bar}")

    print("\n=== SUMMARY ===")
    print(summary)
