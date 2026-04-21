"""Voice note transcription using faster-whisper."""

import base64
import tempfile
import os

from faster_whisper import WhisperModel

_model = None


def _get_model() -> WhisperModel:
    global _model
    if _model is None:
        model_size = os.getenv("WHISPER_MODEL", "base")
        _model = WhisperModel(model_size, device="auto", compute_type="int8")
    return _model


def transcribe_audio(audio_b64: str, mimetype: str = "audio/ogg") -> str:
    """Transcribe base64-encoded audio to text using faster-whisper."""
    ext = ".ogg"
    if "mp4" in mimetype or "m4a" in mimetype:
        ext = ".m4a"
    elif "wav" in mimetype:
        ext = ".wav"
    elif "webm" in mimetype:
        ext = ".webm"

    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as f:
        f.write(base64.b64decode(audio_b64))
        tmp_path = f.name

    try:
        model = _get_model()
        segments, _info = model.transcribe(tmp_path, language="es")
        text = " ".join(seg.text.strip() for seg in segments)
        return text
    finally:
        os.unlink(tmp_path)
