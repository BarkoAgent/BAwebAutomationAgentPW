from __future__ import annotations

from typing import Any, Dict, List

from ..models import (
    COVERAGE_AUTOMATED,
    COVERAGE_SEMI_AUTOMATED,
    OUTCOME_FAILED,
    OUTCOME_NEEDS_REVIEW,
    OUTCOME_NOT_APPLICABLE,
    OUTCOME_PASSED,
)


# WCAG 1.2.x: Time-based Media
# 1.2.1 (A): Audio-only/video-only prerecorded — provide alternative
# 1.2.2 (A): Captions for prerecorded audio in synchronized media
# 1.2.3 (A): Audio description or media alternative for prerecorded video
# 1.2.4 (AA): Captions for live audio
# 1.2.5 (AA): Audio description for prerecorded video
# 1.4.2 (A): Audio control — auto-playing audio can be paused/stopped/muted

MEDIA_SCAN_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  function getTrackKinds(el) {
    return Array.from(el.querySelectorAll('track')).map(t => ({
      kind: t.getAttribute('kind') || '',
      srclang: t.getAttribute('srclang') || '',
      label: t.getAttribute('label') || '',
      src: t.getAttribute('src') || '',
    }));
  }

  function hasCustomControls(el) {
    // Look for player-like ancestor and probe its descendant controls.
    // Broadened to common custom-player root patterns (Plyr, Video.js, JW, Brightcove, etc).
    const playerRoot = el.closest(
      '[class*="player" i], [class*="video" i][class*="container" i], [data-player], ' +
      '[class*="plyr" i], [class*="vjs" i], [class*="jwplayer" i], [class*="brightcove" i], ' +
      '[role="application"]'
    ) || el.parentElement;
    if (!playerRoot) return false;
    const btns = playerRoot.querySelectorAll(
      'button, [role="button"], [aria-label*="play" i], [aria-label*="pause" i], ' +
      '[aria-label*="mute" i], [class*="play-button" i], [class*="pause" i], [class*="control" i]'
    );
    return btns.length > 0;
  }

  function isDecorative(el, isAutoplay, isMuted, hasAudioTrack, hasNativeControls) {
    // Muted, autoplaying, looping, no controls, no audio track → decorative background video.
    // WCAG 1.2.x captions/descriptions don't apply to media without audio.
    const looping = el.hasAttribute('loop') || el.loop;
    return isAutoplay && isMuted && looping && !hasNativeControls && !hasAudioTrack;
  }

  // Walk light DOM + open shadow roots, then dedupe by element identity.
  function collectMedia(root, tag, out, seen) {
    if (!root) return;
    const list = (root.querySelectorAll ? root.querySelectorAll(tag) : []);
    for (const el of list) {
      if (seen.has(el)) continue;
      seen.add(el);
      out.push(el);
    }
    // Recurse into open shadow roots
    const all = (root.querySelectorAll ? root.querySelectorAll('*') : []);
    for (const el of all) {
      if (el.shadowRoot) collectMedia(el.shadowRoot, tag, out, seen);
    }
  }
  const videoSeen = new Set();
  const videoEls = [];
  collectMedia(document, 'video', videoEls, videoSeen);
  const audioSeen = new Set();
  const audioEls = [];
  collectMedia(document, 'audio', audioEls, audioSeen);

  const videos = videoEls.map(el => {
    const tracks = getTrackKinds(el);
    const captionTracks = tracks.filter(t => t.kind === 'captions' || t.kind === 'subtitles');
    const descTracks = tracks.filter(t => t.kind === 'descriptions');
    const hasNativeControls = el.hasAttribute('controls');
    const customControls = hasCustomControls(el);
    const isAutoplay = el.hasAttribute('autoplay');
    const isMuted = el.muted || el.hasAttribute('muted');
    const hasAudioTrack = !el.hasAttribute('muted') && el.volume > 0;
    // Check for nearby transcript link
    const parent = el.closest('figure, div, section, article') || el.parentElement;
    const nearbyLinks = parent
      ? Array.from(parent.querySelectorAll('a')).filter(a => {
          const t = (a.innerText || a.textContent || '').toLowerCase();
          return t.includes('transcript') || t.includes('description') || t.includes('alternative');
        }).map(a => a.getAttribute('href') || '')
      : [];
    return {
      locator: cssPath(el),
      src: el.getAttribute('src') || el.querySelector('source')?.getAttribute('src') || '',
      hasNativeControls,
      customControls,
      isAutoplay,
      isMuted,
      hasAudioTrack,
      isDecorative: isDecorative(el, isAutoplay, isMuted, hasAudioTrack, hasNativeControls),
      tracks,
      captionTracks,
      descTracks,
      hasCaptions: captionTracks.length > 0,
      hasDescriptions: descTracks.length > 0,
      nearbyTranscriptLinks: nearbyLinks,
    };
  });

  const audios = audioEls.map(el => {
    const hasNativeControls = el.hasAttribute('controls');
    const customControls = hasCustomControls(el);
    const isAutoplay = el.hasAttribute('autoplay');
    const isMuted = el.muted || el.hasAttribute('muted');
    // Check for nearby transcript link
    const parent = el.closest('figure, div, section, article') || el.parentElement;
    const nearbyLinks = parent
      ? Array.from(parent.querySelectorAll('a')).filter(a => {
          const t = (a.innerText || a.textContent || '').toLowerCase();
          return t.includes('transcript') || t.includes('alternative');
        }).map(a => a.getAttribute('href') || '')
      : [];
    return {
      locator: cssPath(el),
      src: el.getAttribute('src') || el.querySelector('source')?.getAttribute('src') || '',
      hasNativeControls,
      customControls,
      isAutoplay,
      isMuted,
      nearbyTranscriptLinks: nearbyLinks,
    };
  });

  return { videos, audios };
}
"""


async def run_media_alternatives_evaluator(page: Any) -> List[Dict[str, Any]]:
    try:
        data = await page.evaluate(MEDIA_SCAN_SCRIPT)
    except Exception:
        return []

    videos = data.get("videos", [])
    audios = data.get("audios", [])
    results: List[Dict[str, Any]] = []

    if not videos and not audios:
        return []

    metadata = {"videos": videos, "audios": audios}

    # --- 1.4.2: Audio Control (auto-playing audio with volume) ---
    autoplay_audio = [
        a for a in audios
        if a.get("isAutoplay") and not a.get("isMuted")
    ]
    autoplay_video_audio = [
        v for v in videos
        if v.get("isAutoplay") and not v.get("isMuted") and v.get("hasAudioTrack", True)
        and not v.get("hasNativeControls") and not v.get("customControls")
    ]

    if autoplay_audio or autoplay_video_audio:
        failing = autoplay_audio or autoplay_video_audio
        first = failing[0]
        results.append(
            {
                "criterion_id": "1.4.2",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "critical",
                "message": (
                    "Auto-playing media with audio detected without a mechanism to pause, "
                    "stop, or mute it, violating WCAG 1.4.2."
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("src", ""),
                "metadata": {**metadata, "autoplay_without_control": failing},
            }
        )

    # --- 1.2.2 / 1.2.5: Captions and Audio Descriptions for video ---
    # Decorative (muted+autoplay+loop, no audio track, no controls) video does not
    # carry meaningful audio content — captions / descriptions don't apply.
    eligible_videos = [v for v in videos if not v.get("isDecorative")]
    videos_missing_captions = [v for v in eligible_videos if not v.get("hasCaptions")]
    videos_missing_desc = [v for v in eligible_videos if not v.get("hasDescriptions") and not v.get("nearbyTranscriptLinks")]

    if videos_missing_captions:
        first = videos_missing_captions[0]
        results.append(
            {
                "criterion_id": "1.2.2",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "critical",
                "message": (
                    "{} video element(s) found without a <track kind='captions'> or "
                    "<track kind='subtitles'> element, violating WCAG 1.2.2.".format(len(videos_missing_captions))
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("src", ""),
                "metadata": {**metadata, "missing_captions": videos_missing_captions},
            }
        )
    elif eligible_videos:
        results.append(
            {
                "criterion_id": "1.2.2",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": "All detected video elements have a captions or subtitles track.",
                "locator": eligible_videos[0].get("locator", ""),
                "element_text": "",
                "metadata": metadata,
            }
        )
    elif videos:
        # Videos exist but all are decorative (muted/looping, no audio track) —
        # captions don't apply. Report as Not Applicable, not a pass.
        results.append(
            {
                "criterion_id": "1.2.2",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_NOT_APPLICABLE,
                "severity": "minor",
                "message": "Only decorative video (muted, no audio track) detected — captions not applicable.",
                "locator": videos[0].get("locator", ""),
                "element_text": "",
                "metadata": metadata,
            }
        )

    if videos_missing_desc:
        first = videos_missing_desc[0]
        results.append(
            {
                "criterion_id": "1.2.5",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "serious",
                "message": (
                    "{} video element(s) found without a <track kind='descriptions'> element "
                    "or nearby transcript link. Verify audio description is provided for WCAG 1.2.5.".format(
                        len(videos_missing_desc)
                    )
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("src", ""),
                "metadata": {**metadata, "missing_descriptions": videos_missing_desc},
            }
        )
    elif eligible_videos:
        results.append(
            {
                "criterion_id": "1.2.5",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "Video elements have description tracks or nearby transcript links. "
                    "Verify description content accurately represents all visual information."
                ),
                "locator": eligible_videos[0].get("locator", ""),
                "element_text": "",
                "metadata": metadata,
            }
        )
    elif videos:
        # Only decorative video present — audio description does not apply.
        results.append(
            {
                "criterion_id": "1.2.5",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_NOT_APPLICABLE,
                "severity": "minor",
                "message": "Only decorative video (muted, no audio track) detected — audio description not applicable.",
                "locator": videos[0].get("locator", ""),
                "element_text": "",
                "metadata": metadata,
            }
        )

    # --- 1.2.1: Audio-only prerecorded — check for transcript links ---
    audios_missing_transcript = [a for a in audios if not a.get("nearbyTranscriptLinks")]
    if audios_missing_transcript:
        first = audios_missing_transcript[0]
        results.append(
            {
                "criterion_id": "1.2.1",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "serious",
                "message": (
                    "{} audio element(s) found without a nearby transcript link. "
                    "Verify a text alternative exists for all prerecorded audio (WCAG 1.2.1).".format(
                        len(audios_missing_transcript)
                    )
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("src", ""),
                "metadata": {**metadata, "audio_missing_transcript": audios_missing_transcript},
            }
        )
    elif audios:
        results.append(
            {
                "criterion_id": "1.2.1",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "Audio elements appear to have nearby transcript links. "
                    "Verify the transcript fully represents the audio content."
                ),
                "locator": audios[0].get("locator", ""),
                "element_text": "",
                "metadata": metadata,
            }
        )

    # --- Controls check: no native or custom controls ---
    media_without_controls = [
        v for v in videos if not v.get("hasNativeControls") and not v.get("customControls")
    ] + [
        a for a in audios if not a.get("hasNativeControls") and not a.get("customControls")
    ]
    if media_without_controls:
        first = media_without_controls[0]
        results.append(
            {
                "criterion_id": "1.2.1",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "{} media element(s) have no native controls attribute and no "
                    "detectable custom player controls nearby. Users may not be able to "
                    "control playback.".format(len(media_without_controls))
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("src", ""),
                "metadata": {**metadata, "no_controls": media_without_controls},
            }
        )

    return results
