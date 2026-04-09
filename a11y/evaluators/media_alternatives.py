from __future__ import annotations

from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED


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
    // Check if parent has controls-like buttons nearby
    const parent = el.parentElement;
    if (!parent) return false;
    const btns = parent.querySelectorAll(
      'button, [role="button"], [aria-label*="play" i], [aria-label*="pause" i], [aria-label*="mute" i]'
    );
    return btns.length > 0;
  }

  const videos = Array.from(document.querySelectorAll('video')).map(el => {
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
      tracks,
      captionTracks,
      descTracks,
      hasCaptions: captionTracks.length > 0,
      hasDescriptions: descTracks.length > 0,
      nearbyTranscriptLinks: nearbyLinks,
    };
  });

  const audios = Array.from(document.querySelectorAll('audio')).map(el => {
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
    videos_missing_captions = [v for v in videos if not v.get("hasCaptions")]
    videos_missing_desc = [v for v in videos if not v.get("hasDescriptions") and not v.get("nearbyTranscriptLinks")]

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
    elif videos:
        results.append(
            {
                "criterion_id": "1.2.2",
                "source": "custom:media_alternatives",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": "All detected video elements have a captions or subtitles track.",
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
    elif videos:
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
