"""Emplifi per-network endpoint paths, metric names, and post field lists."""

# Per-network post endpoint paths.
POSTS_ENDPOINT = {
    "facebook": "/3/facebook/page/posts",
    "instagram": "/3/instagram/profile/posts",
    "twitter": "/3/twitter/profile/tweets",
    "youtube": "/3/youtube/profile/videos",
    "linkedin": "/3/linkedin/profile/posts",
    "pinterest": "/3/pinterest/profile/posts",
    "tiktok": "/3/tiktok/profile/posts",
    "snapchat": "/3/snapchat/profile/posts",
}

# Per-network valid profile metrics, from the Emplifi API docs metric table. A network not
# listed here (e.g. snapchat, whatsapp) has no supported profile metrics and is skipped in
# step 2.
PROFILE_METRICS = {
    "facebook": [
        "fans_lifetime",
        "fans_change",
        "insights_fan_adds",
        "insights_fan_removes",
        "insights_impressions",
        "insights_reach",
        "insights_reactions",
        "insights_follows",
    ],
    "instagram": [
        "followers_lifetime",
        "followers_change",
        "following_lifetime",
        "following_change",
        "insights_followers",
        "insights_impressions",
        "insights_media_views",
        "insights_profile_views",
    ],
    "twitter": [
        "followers_lifetime",
        "followers_change",
        "following_lifetime",
    ],
    "youtube": [
        "subscribers_lifetime",
        "subscribers_change",
        "views_change",
        "viewed_time_change",
        "video_lifetime",
    ],
    "linkedin": [
        "followers_lifetime",
        "followers_change",
        "insights_impressions",
    ],
    "pinterest": [
        "followers_lifetime",
    ],
    "tiktok": [
        "insights_fans_lifetime",
        "insights_fans_change",
        "insights_video_views",
        "insights_profile_views",
    ],
}

# Per-network valid post fields, sourced from the Emplifi API docs per-network posts section.
# Only confirmed-valid fields are included per network to avoid Code-3 validation errors.
POST_FIELDS_BY_NETWORK = {
    # /3/facebook/page/posts - reactions, comments, shares, interactions, post_attribution
    # are all documented as valid.
    "facebook": [
        "id",
        "created_time",
        "content",
        "url",
        "media_type",
        "interactions",
        "reactions",
        "comments",
        "shares",
        "post_attribution",
        "profileId",
    ],
    # /3/instagram/profile/posts - comments (not number_of_comments), likes, and
    # interactions are all documented as valid.
    "instagram": [
        "id",
        "created_time",
        "content",
        "url",
        "media_type",
        "interactions",
        "likes",
        "comments",
        "profileId",
    ],
    # /3/twitter/profile/tweets - per Twitter's Developer Policy, Emplifi no longer provides
    # individual tweet details. Only these fields are allowed: no metrics (no created_time,
    # interactions, likes, etc.).
    "twitter": [
        "id",
        "profileId",
        "origin",
    ],
    # /3/youtube/profile/videos - documented fields: comments, created_time, id,
    # interactions, likes, url, video_views, video_view_time, duration, profileId.
    # title is invalid (use description); number_of_comments and insights_video_views
    # are also invalid here.
    "youtube": [
        "id",
        "created_time",
        "url",
        "description",
        "interactions",
        "likes",
        "comments",
        "video_views",
        "video_view_time",
        "duration",
        "profileId",
    ],
    # /3/linkedin/profile/posts - documented fields: comments, reactions, interactions,
    # content, created_time, id, url. likes is invalid (LinkedIn uses reactions).
    "linkedin": [
        "id",
        "created_time",
        "content",
        "url",
        "media_type",
        "interactions",
        "reactions",
        "comments",
        "profileId",
    ],
    # /3/pinterest/profile/posts - kept to a minimal safe set; few profiles use this network.
    "pinterest": [
        "id",
        "created_time",
        "url",
        "interactions",
        "comments",
    ],
    # /3/tiktok/profile/posts - basic fields per the docs: id, created_time, link, media,
    # message, duration, content_type (no profileId, no interactions, no likes in basic
    # fields). insights_ fields require insights_enabled=true on the profile; they fail
    # gracefully (Code-7) per-profile when not enabled.
    "tiktok": [
        "id",
        "created_time",
        "link",
        "media",
        "message",
        "duration",
        "insights_video_views",
        "insights_engagements",
        "insights_likes",
        "insights_comments",
        "insights_shares",
    ],
}

# Cross-platform aggregated metrics for /3/aggregated-metrics, with names taken directly
# from the Emplifi API docs metric table. Invalid names that cause Code-3 errors: reactions,
# comments, video_views, reach, shares - the correct equivalents are number_of_comments,
# page_shares, insights_video_views, etc.
AGGREGATED_METRICS = [
    "interactions",
    "likes",
    "number_of_comments",
    "page_posts",
    "insights_engagements",
    "insights_video_views",
    "insights_impressions",
    "page_shares",
    "engagement_rate",
]
