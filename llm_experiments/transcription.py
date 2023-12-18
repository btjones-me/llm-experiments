# !pip install --upgrade google-cloud-speech
# !pip install pydub
from google.cloud import speech
from pydub import AudioSegment
from pathlib import Path as path

from llm_experiments.utils import run_command, here

proj = run_command("!gcloud config set project {name}", name="motorway-genai")


# Load the audio file from the provided path
audio_path = here() / "data/audio/S3T06.wav"


def transcribe_gcs(gcs_uri: str) -> str:
    """Asynchronously transcribes the audio file specified by the gcs_uri.

    Args:
        gcs_uri: The Google Cloud Storage path to an audio file.

    Returns:
        The generated transcript from the audio file provided.
    """

    client = speech.SpeechClient()

    audio = speech.RecognitionAudio(uri=gcs_uri)
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
        # sample_rate_hertz=44100,
        language_code="en-UK",
    )

    operation = client.long_running_recognize(config=config, audio=audio)

    print("Waiting for operation to complete...")
    response = operation.result(timeout=180)

    transcript_builder = []
    # Each result is for a consecutive portion of the audio. Iterate through
    # them to get the transcripts for the entire audio file.
    for result in response.results:
        # The first alternative is the most likely one for this portion.
        transcript_builder.append(f"\nTranscript: {result.alternatives[0].transcript}")
        transcript_builder.append(f"\nConfidence: {result.alternatives[0].confidence}")

    transcript = "".join(transcript_builder)
    print(transcript)

    return transcript


audio = AudioSegment.from_file(audio_path)
# Check if the first channel has content by splitting the audio into its separate channels
channels = audio.split_to_mono()
# Save the first channel to a separate file
isolated_channel_path = (
    here() / f"data/audio/{path(audio_path).stem}_isolated_channel2.wav"
)
# Now, channels[0] is a mono audio segment of the first channel
# set_sample_width(2) ensures that the audio is 16-bit
channels[0].set_sample_width(2).export(isolated_channel_path, format="wav")

gcs_out_uri = f"gs://gen-ai-test-playground/audio-files-marketing/{path(isolated_channel_path).name}"


print(f"{isolated_channel_path=}")
print(f"{gcs_out_uri=}")

# upload file to gcs bucket
run_command(f"!gsutil cp {isolated_channel_path} {gcs_out_uri}")
transcript = transcribe_gcs(gcs_out_uri)
