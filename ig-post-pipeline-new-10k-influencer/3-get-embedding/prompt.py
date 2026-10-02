"""Gemini extraction prompt for Instagram caption metadata."""

EXTRACTION_PROMPT = """\
You are analyzing an Instagram post caption (it may include transcript-like text).
Return ONLY valid JSON, no markdown, no extra text.

CRITICAL RULE: ALL TEXT OUTPUT MUST BE IN ENGLISH. Tags, categories, and all
other text fields MUST be English only. Even if the caption is entirely Chinese,
Japanese, Korean, or any other language, your outputs must be in English.
This is non-negotiable. Exactly 10 tags are required.

If a proper noun or name contains non-English characters (e.g., "Brian在日本住"),
transliterate or translate it to English: "Brian在日本住" → "Brian In Japan".
Do NOT leave any non-English text in any output field. All 10 tags must be
composed entirely of A-Z, a-z, digits 0-9, and spaces only.

AMBIGUOUS TERMS RULE: Many place names, cultural terms, and landmarks share
the same name across different countries and languages (e.g., "東北" = NE China
vs Tohoku Japan; "Cambridge" = UK vs US; "Sydney" = Australia vs Canada; 
"Georgia" = US state vs country). Use the caption's language, grammar, 
colloquialisms, food references, and cultural context to determine the correct
interpretation. When a caption is written in Chinese (Cantonese/Mandarin) with
Chinese cultural references, prefer Chinese geographic interpretations. When
written in Japanese with Japanese context, prefer Japanese interpretations.
If the language and cultural signals are unclear, choose the most likely
global interpretation.

Task A: Generate exactly 10 topic tags in English.

Goal:
Produce high-level, stable THEMES for content discovery. Tags must be specific
enough to be useful, but not random words.

CRITICAL: These tags are used for SEMANTIC SEARCH FILTERING. Generic tags
like "Travel Update" make the filter useless. Specific tags like "Japan Road
Trip" or "Okinawa Beach" make search actually work.

Hard grounding rules (no hallucination):
- Every tag MUST be directly supported by the caption text.
- If the caption does not clearly support a tag, DO NOT output it.
- Do NOT invent names, roles, locations, dates, or events.
- Do NOT copy arbitrary phrases; prefer normalized themes.

Tag style rules:
- Exactly 10 tags.
- Each tag is 1 to 4 words.
- Use only letters A-Z/a-z, digits 0-9, and spaces.
- No emojis, no punctuation, no symbols, no hashtags, no non-Latin characters.
- Use Title Case for all tags.

STRICTLY BANNED filler tags (DO NOT USE):
General Topic, Lifestyle, Trending Topic, Pop Culture, Creator Content,
Daily Life, Social Media, Video Content, Entertainment, Travel Update,
Personal Thoughts, Vacation, Journey, Travel Experience, Travel Vlog,
Holiday, Travel Diary, Travel Lifestyle, Travel Planning, Travel Tips,
Daily Update, Life Update, Personal Growth, Life Reflection, Mindfulness,
Positive Thinking, Travel Inspiration, Travel Memories, Travel Community.

DO NOT use meta tags about the post format unless clearly central
(e.g., Vlog, Tutorial, Review, Behind The Scenes).

How to choose tags — DISCRIMINATION RULE:
Pick tags that would help DISTINGUISH this post from other similar posts.
"Travel" matches millions of posts. "Okinawa Road Trip" matches only this one.

1) First infer ONE main theme and 2 to 4 secondary themes from the caption.
2) ALWAYS include a LOCATION tag if a place is mentioned or clearly implied
   (e.g., Tokyo, Okinawa, Thailand, Hong Kong, Bali).
3) Prefer tags from these theme types when present:
   - Activity or domain: Scuba Diving, Hiking, Street Photography, Cafe Hopping
   - Specific place or region ONLY if explicitly stated: Tokyo, Okinawa, Seoul
   - Media or genre ONLY if explicit: Travel Vlog, Food Review, Kpop
   - Food or item ONLY if explicit: Ramen, Bubble Tea, Sushi
   - Occasion ONLY if explicit: Christmas, Cherry Blossom Season
   - Emotion or self-development ONLY if explicit and central: Solo Travel,
     Personal Challenge
4) If the caption is too short or vague, fall back to broader grounded tags:
   Daily Update, Personal Thoughts, Work Life, Family Moment, Food Moment,
   Music Clip, TV Show, Shopping Trip, City Walk, Pet Moment, Fitness Update
   — but still prefer specific location + activity combos over these.

Quality constraints:
- Tags must be distinct (no near-duplicates).
- Do not include Part One, Part Two, Episode Numbers, or similar sequencing.
- Do not include single letters or single generic words like Nice, Happy, Good.
- If a brand is mentioned, you may include a brand-related tag ONLY if it
  represents the content theme (e.g., Nike Running), otherwise omit.
- At least 3 of the 10 tags should be SPECIFIC enough to uniquely identify
  this post's main subject (e.g., "Okinawa Diving" not just "Ocean").

Task B: Extract brand names mentioned in the caption.
- Identify the formal names of brands mentioned (e.g., 'Nike', 'Disney',
  'Coca-Cola').
- These are natural language names, not handles.
- Output as a list of strings. Empty list if none.

Task C: Extract Instagram usernames that are mentioned in the caption and
separate BRANDS vs INFLUENCERS.
Act as a Social Media Data Miner. Your goal is to extract Instagram usernames
that are mentioned without the @ symbol. You must be extremely strict to avoid
extracting normal words, names, or titles.

Candidate extraction rules:
- Only consider tokens that are ALL LOWERCASE and contain '_' or '.'
  (e.g., 'elsie_lui', 'thegrand_hk').
- Also extract if two lowercase tokens appear together (cluster pattern),
  e.g., 'bakerybythegrand thegrand_hk'.
- Also extract lowercase tokens immediately after Chinese words like '同' or
  '感謝' if they look like usernames and are not dictionary words.

Exclusion rules (CRITICAL):
- If a word starts with a Capital Letter, it is a proper noun/name; DO NOT
  extract.
- Ignore event names and common nouns.
- Ignore single common first names.

Now classify each extracted username into one of two lists:
1) associated_brand: brand/company/shop/venue/product/service accounts.
2) associated_mention: influencer/creator/personal accounts.

Classification heuristics (use strict best-effort):
- Put into associated_brand if the username contains business/brand indicators
  such as: hk, hkg, official, shop, store, mall, hotel, restaurant, cafe, bar,
  dining, studio, salon, clinic, spa, beauty, skincare, makeup, cosmetics,
  fashion, jewelry, watch, travel, tours, airline, bank, insurance, comms, pr,
  agency, media, group, ltd, co, company, brand, boutique, bakery, kitchen,
  grill, izakaya, ramen, pizza, coffee, tea, dessert, sports, football, adidas,
  nike, puma, uniqlo, disney, hermes, dior, sephora, zeiss, owndays, bvlgari.
- Put into associated_brand if the caption context around it is
  promotional/brand-like: 'shop', 'link in bio', 'code', 'discount', 'book',
  'reservation', 'available at', 'now at', 'menu', 'treatment', 'package',
  'launch', 'drop'.
- Put into associated_mention if the username looks like a person/creator:
  contains a personal name pattern, or creator indicators such as: mua,
  makeupartist, artist, photographer, photo, videographer, editor, stylist,
  hair, nails, coach, trainer, dancer, actor, singer, model, dj, yoga.
- If uncertain, default to associated_mention unless there is a clear
  brand/business indicator.

Output requirements:
- Output extracted usernames WITHOUT leading '@'.
- Each entry must be ONE token with NO spaces.
- Allowed characters: letters a-z, digits 0-9, underscore _, dot ., and
  hyphen -.
- Deduplicate per list (case-insensitive dedupe OK, output first-seen form).
- If none, output empty list [].

Task D: Identify which words in the caption were originally hashtags.
- Output the hashtag words WITHOUT the # symbol.
- Only include words that were clearly used as hashtags in the original caption.
- If none, output empty list [].

Task E: Classify is_sponsorship.
is_sponsorship MUST be either 1 (true) or 0 (false).

Goal: In real Instagram captions, sponsorship is often IMPLIED. You must not be
overly conservative. Set is_sponsorship=1 when the caption is promoting a
brand/product/service and includes a brand reference.

Primary rule (most common case):
Set is_sponsorship=1 if associated_brand is NOT empty AND the caption contains
ANY promotional intent.

Promotional intent includes ANY of the following (treat as strong):
- Call to action: shop, order, buy, purchase, pre order, available now, drop,
  launch, link in bio, check out, tap, click, swipe, DM to order, dm me,
  inbox, whatsapp, book now, booking, appointment, reserve
- Price or offer: $, %, off, discount, promo, voucher, deal, sale, limited
  time, special offer, free delivery, code, use code, referral
- Product/service push: try this, must have, highly recommend, new product,
  new menu, new treatment, package, set, combo, best seller
- Stock/location: available at, now at, in store, online, website, hotline
- Brand shoutout patterns: thanks, thank you, shoutout followed by a
  brand/handle
- Partnership words: ad, sponsored, paid partnership, collab, collaboration,
  partnered with, ambassador, affiliate, gifted, PR, sent me

Secondary rule (when no associated_brand found):
Set is_sponsorship=1 if brand_name is NOT empty AND the caption contains ANY
of:
- explicit partnership words (ad, sponsored, paid partnership, collab,
  ambassador, affiliate, gifted, PR)
- a promo mechanic (code, discount, voucher, referral, link in bio)
- a clear call to action (shop, order, book now, DM to order)

Do NOT set is_sponsorship=1 only for:
- Pure personal opinion with no promotion
  (e.g., 'I love Nike shoes')
- Pure event attendance or generic tags with no selling/CTA

Important bias rule:
If there is ANY doubt and there is a brand reference (brand_name or
associated_brand) PLUS any promotional intent word, choose is_sponsorship=1.

Task F: Classify whether the caption contains specific, searchable content.
caption_is_specific MUST be either 1 (true) or 0 (false).

Set caption_is_specific=1 if the caption describes a specific:
- Activity, event, or experience (e.g., "Hiked Mount Fuji at sunrise")
- Place, product, or item (e.g., "Best ramen in Tokyo")
- Person, brand, or media (e.g., "Watching the new Dune movie")
- Opinion or review with substance (e.g., "This camera is amazing for vlogging")

Set caption_is_specific=0 if the caption is:
- A generic greeting or sign-off (e.g., "Good morning", "Hello everyone")
- Vague or cryptic (e.g., "Well...it just happened", "Life is good")
- Only emojis, single words, or filler (e.g., "❤️", "Thanks", "Happy")
- A generic quote or proverb without context
- Purely self-referential with no subject (e.g., "Just thinking", "Random thoughts")
- VERY SHORT text that doesn't clearly describe an activity, place, or thing
  (e.g., "Dating jap 🤗", "Japan 🤗", "Nice day", "Travel time")
  — even if it contains a location keyword like "Japan", the text is too
  short to infer meaningful searchable content.

Task G: Classify the post into exactly one category.
category MUST be exactly one of the following strings:
- BEAUTY_MAKEUP — makeup tutorial, skincare, cosmetics review, beauty product
- TRAVEL — travel diary, destination guide, trip planning, sightseeing
- FOOD — restaurant review, recipe, cooking, food photo, cafe
- FASHION — outfit, style, shopping haul, accessories, OOTD
- MUSIC — song, instrument, concert, music video, artist
- PERSONAL — personal update, life reflection, daily vlog, family
- SHOPPING — product promotion, deal, discount, haul, e-commerce
- EDUCATION — tutorial, how-to, tips, learning (non-beauty)
- NEWS — news, current events, information share
- SPORTS — fitness, workout, sports event, athlete
- PET — animal, pet, cat, dog
- OTHER — none of the above

Choose ONE category that best describes the post's PRIMARY content.
If a post is about a makeup tutorial, category is BEAUTY_MAKEUP even if travel is briefly mentioned.

Output schema exactly (ALL text values MUST be in English):
{"tags":["tag1","tag2",...,"tag10"],"brand_name":["brand1",...],"associated_brand":["handle1",...],"associated_mention":["handle1",...],"hashtags":["hashtag1",...],"is_sponsorship":0,"caption_is_specific":1,"category":"BEAUTY_MAKEUP"}

<caption>
{CAPTION_TEXT}
</caption>"""