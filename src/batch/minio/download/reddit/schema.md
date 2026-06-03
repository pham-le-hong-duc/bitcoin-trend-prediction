# Reddit Data Schema

Danh sách các fields có sẵn trong Reddit Pushshift dumps (.zst files).

## 📝 Comments Fields (RC_*.zst)

### Core Fields
| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique comment ID |
| `author` | string | Username of comment author |
| `subreddit` | string | Subreddit name (without /r/) |
| `body` | string | Comment text content |
| `created_utc` | timestamp | Unix timestamp when comment was created |
| `link_id` | string | Submission ID this comment belongs to (prefix: `t3_`) |
| `parent_id` | string | Parent comment/submission ID (prefix: `t1_` or `t3_`) |

### Engagement Fields
| Field | Type | Description |
|-------|------|-------------|
| `score` | integer | Comment score (upvotes - downvotes) |
| `ups` | integer | Number of upvotes |
| `downs` | integer | Number of downvotes |
| `controversiality` | integer | Controversiality score (0 or 1) |
| `gilded` | integer | Number of Reddit Gold/Awards |
| `all_awardings` | array | List of all awards received |
| `total_awards_received` | integer | Total number of awards |

### Author Flair Fields
| Field | Type | Description |
|-------|------|-------------|
| `author_flair_text` | string | Author's flair text |
| `author_flair_css_class` | string | CSS class for flair |
| `author_flair_richtext` | array | Rich text flair data |
| `author_flair_type` | string | Type of flair (text/richtext) |
| `author_flair_background_color` | string | Flair background color |
| `author_flair_text_color` | string | Flair text color |
| `author_flair_template_id` | string | Template ID for flair |

### Metadata Fields
| Field | Type | Description |
|-------|------|-------------|
| `permalink` | string | Relative URL to comment |
| `retrieved_on` | timestamp | When this data was archived |
| `edited` | boolean/timestamp | False or timestamp of edit |
| `stickied` | boolean | Whether comment is stickied |
| `locked` | boolean | Whether comment is locked |
| `archived` | boolean | Whether comment is archived |
| `distinguished` | string | "moderator" or "admin" if applicable |
| `is_submitter` | boolean | Whether author is OP |

### Moderation Fields
| Field | Type | Description |
|-------|------|-------------|
| `approved_at_utc` | timestamp | When approved by mod |
| `approved_by` | string | Moderator who approved |
| `banned_at_utc` | timestamp | When removed/banned |
| `banned_by` | string | Moderator who removed |
| `removed_by_category` | string | Removal category |
| `can_mod_post` | boolean | Whether user can moderate |

### User Fields
| Field | Type | Description |
|-------|------|-------------|
| `author_fullname` | string | Full Reddit user ID (prefix: `t2_`) |
| `author_premium` | boolean | Whether author has Reddit Premium |
| `author_patreon_flair` | boolean | Patreon supporter flair |
| `author_is_blocked` | boolean | Whether author is blocked |

### Display Fields
| Field | Type | Description |
|-------|------|-------------|
| `collapsed` | boolean | Whether comment is collapsed |
| `collapsed_reason` | string | Reason for collapse |
| `collapsed_because_crowd_control` | boolean | Collapsed by crowd control |
| `score_hidden` | boolean | Whether score is hidden |

### Technical Fields
| Field | Type | Description |
|-------|------|-------------|
| `_meta` | object | Metadata about the record |
| `subreddit_id` | string | Subreddit ID (prefix: `t5_`) |
| `name` | string | Full comment name (prefix: `t1_`) |
| `treatment_tags` | array | Treatment tags |

---

## 📰 Submissions Fields (RS_*.zst)

### Core Fields
| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique submission ID |
| `author` | string | Username of submission author |
| `subreddit` | string | Subreddit name (without /r/) |
| `title` | string | Submission title |
| `selftext` | string | Self-post text content (empty for link posts) |
| `created_utc` | timestamp | Unix timestamp when submitted |
| `url` | string | URL of the submission |
| `domain` | string | Domain of the link |
| `is_self` | boolean | Whether it's a self-post (text post) |

### Engagement Fields
| Field | Type | Description |
|-------|------|-------------|
| `score` | integer | Submission score (upvotes - downvotes) |
| `ups` | integer | Number of upvotes |
| `downs` | integer | Number of downvotes |
| `upvote_ratio` | float | Ratio of upvotes (0.0 - 1.0) |
| `num_comments` | integer | Number of comments |
| `gilded` | integer | Number of Reddit Gold/Awards |
| `all_awardings` | array | List of all awards received |
| `total_awards_received` | integer | Total number of awards |

### Link Flair Fields
| Field | Type | Description |
|-------|------|-------------|
| `link_flair_text` | string | Link flair text |
| `link_flair_css_class` | string | CSS class for link flair |
| `link_flair_richtext` | array | Rich text flair data |
| `link_flair_type` | string | Type of flair |
| `link_flair_background_color` | string | Flair background color |
| `link_flair_text_color` | string | Flair text color |
| `link_flair_template_id` | string | Template ID for flair |

### Author Flair Fields
| Field | Type | Description |
|-------|------|-------------|
| `author_flair_text` | string | Author's flair text |
| `author_flair_css_class` | string | CSS class for author flair |
| `author_flair_richtext` | array | Rich text flair data |
| `author_flair_type` | string | Type of flair |
| `author_flair_background_color` | string | Flair background color |
| `author_flair_text_color` | string | Flair text color |
| `author_flair_template_id` | string | Template ID for flair |

### Media Fields
| Field | Type | Description |
|-------|------|-------------|
| `thumbnail` | string | URL to thumbnail image |
| `thumbnail_height` | integer | Thumbnail height in pixels |
| `thumbnail_width` | integer | Thumbnail width in pixels |
| `preview` | object | Preview images data |
| `media` | object | Embedded media data |
| `media_embed` | object | Media embed data |
| `secure_media` | object | Secure media data |
| `secure_media_embed` | object | Secure media embed data |
| `is_video` | boolean | Whether submission is a video |
| `is_gallery` | boolean | Whether submission is a gallery |
| `gallery_data` | object | Gallery images data |
| `media_metadata` | object | Metadata for media items |

### Metadata Fields
| Field | Type | Description |
|-------|------|-------------|
| `permalink` | string | Relative URL to submission |
| `retrieved_on` | timestamp | When this data was archived |
| `edited` | boolean/timestamp | False or timestamp of edit |
| `stickied` | boolean | Whether submission is stickied |
| `locked` | boolean | Whether submission is locked |
| `archived` | boolean | Whether submission is archived |
| `distinguished` | string | "moderator" or "admin" if applicable |
| `pinned` | boolean | Whether submission is pinned |
| `over_18` | boolean | Whether submission is NSFW |
| `spoiler` | boolean | Whether submission contains spoilers |
| `contest_mode` | boolean | Whether contest mode is enabled |
| `quarantine` | boolean | Whether submission is quarantined |

### Moderation Fields
| Field | Type | Description |
|-------|------|-------------|
| `approved_at_utc` | timestamp | When approved by mod |
| `approved_by` | string | Moderator who approved |
| `banned_at_utc` | timestamp | When removed/banned |
| `banned_by` | string | Moderator who removed |
| `removed_by_category` | string | Removal category |
| `can_mod_post` | boolean | Whether user can moderate |
| `removal_reason` | string | Reason for removal |
| `mod_note` | string | Moderator note |
| `mod_reason_by` | string | Moderator who gave reason |
| `mod_reason_title` | string | Title of mod reason |
| `mod_reports` | array | Moderator reports |
| `user_reports` | array | User reports |
| `num_reports` | integer | Number of reports |

### User Fields
| Field | Type | Description |
|-------|------|-------------|
| `author_fullname` | string | Full Reddit user ID (prefix: `t2_`) |
| `author_premium` | boolean | Whether author has Reddit Premium |
| `author_patreon_flair` | boolean | Patreon supporter flair |
| `author_is_blocked` | boolean | Whether author is blocked |

### Interaction Fields
| Field | Type | Description |
|-------|------|-------------|
| `clicked` | boolean | Whether link was clicked |
| `hidden` | boolean | Whether submission is hidden |
| `saved` | boolean | Whether submission is saved |
| `visited` | boolean | Whether submission was visited |
| `can_gild` | boolean | Whether can give awards |
| `allow_live_comments` | boolean | Whether live comments allowed |

### Technical Fields
| Field | Type | Description |
|-------|------|-------------|
| `_meta` | object | Metadata about the record |
| `subreddit_id` | string | Subreddit ID (prefix: `t5_`) |
| `name` | string | Full submission name (prefix: `t3_`) |
| `full_link` | string | Full URL to submission |
| `post_hint` | string | Type hint for post (image/link/etc) |
| `whitelist_status` | string | Whitelist status |
| `wls` | integer | Whitelist score |
| `pwls` | integer | Platform whitelist score |
| `suggested_sort` | string | Suggested comment sort |
| `category` | string | Submission category |
| `content_categories` | array | Content categories |
| `treatment_tags` | array | Treatment tags |

---

## 🔍 Common Fields Between Both

Both comments and submissions share these fields:
- `id`, `author`, `subreddit`, `created_utc`
- `score`, `ups`, `downs`, `gilded`
- `author_flair_*` fields
- `edited`, `stickied`, `locked`, `archived`, `distinguished`
- `approved_*`, `banned_*` moderation fields
- `author_fullname`, `author_premium`, `author_is_blocked`
- `_meta`, `subreddit_id`

## 📌 Special Fields (Computed by Base Class)

These are special fields handled by `RedditZstFilter._get_field_value()`:

| Field | Description | Example |
|-------|-------------|---------|
| `created` | Formatted timestamp | "2025-12-01 14:30:45" |
| `created_date` | Date only | "2025-12-01" |
| `link` or `permalink_full` | Full Reddit URL | "https://www.reddit.com/r/python/..." |
| `author_prefixed` | Username with prefix | "u/username" |
| `text` | Auto-select body (comments) or selftext (submissions) | Comment/post text |

---

## 💡 Usage Notes

1. **Timestamps**: All `*_utc` fields are Unix timestamps (seconds since epoch)
2. **Prefixes**: Reddit uses prefixes for IDs:
   - `t1_` = comment
   - `t2_` = user
   - `t3_` = submission
   - `t5_` = subreddit
3. **Deleted Content**: 
   - Deleted authors show as `[deleted]`
   - Removed content may have `[removed]` in body/selftext
4. **Empty Fields**: Some fields may be `null` or empty depending on submission type
5. **Arrays**: Fields like `all_awardings`, `user_reports` are arrays and may need special handling in CSV export
