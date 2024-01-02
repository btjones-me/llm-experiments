from google.cloud import speech
from pydub import AudioSegment
from pathlib import Path
from google.cloud import storage
from loguru import logger
from llm_experiments.utils import here


def split_and_convert_audio(file_path, output_format="wav", sample_width=2):
    """
    Splits a stereo audio file into mono and converts it to specified format and bit depth.

    Args:
        file_path (Path): Path to the input audio file.
        output_format (str): The desired output format (default is 'wav').
        sample_width (int): The desired sample width in bytes (default is 2 for 16-bit).

    Returns:
        Path: Path to the converted audio file.
    """
    audio = AudioSegment.from_file(file_path)
    logger.info(f"Splitting audio to mono from {file_path=}")
    channels = audio.split_to_mono()
    output_path = (
        here() / f"{file_path.parent / file_path.stem}_isolated_channel.{output_format}"
    )
    # Export the first channel and set to 16-bit
    channels[0].set_sample_width(sample_width).export(output_path, format=output_format)
    return output_path


def upload_to_gcs(local_file_path, gcs_uri):
    """
    Uploads a file to Google Cloud Storage.

    Args:
    local_file_path (str): The path to the local file to be uploaded.
    gcs_uri (str): The GCS URI where the file will be uploaded, in the format 'gs://bucket_name/path/to/object'.
    """
    storage_client = storage.Client()
    bucket_name, object_name = gcs_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file_path)
    logger.info(f"File {local_file_path} uploaded to {gcs_uri}.")


def transcribe_gcs(gcs_uri: str) -> str:
    """
    Asynchronously transcribes the audio file specified by the gcs_uri.
    """
    client = speech.SpeechClient()
    audio = speech.RecognitionAudio(uri=gcs_uri)
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
        language_code="en-UK",
    )
    operation = client.long_running_recognize(config=config, audio=audio)
    logger.info("Waiting for operation to complete...")
    response = operation.result(timeout=6000)

    confidence_transcript = "\n".join(
        [
            f"Transcript: {result.alternatives[0].transcript}\nConfidence: {result.alternatives[0].confidence}"
            for result in response.results
        ]
    )
    logger.trace(confidence_transcript)
    # Joining all transcripts into a single string
    complete_transcript = " ".join(
        [result.alternatives[0].transcript for result in response.results]
    )
    return complete_transcript, confidence_transcript


# Main script
if __name__ == "__main__":
    # read all files in file_path
    # for each file, split into mono and convert to 16-bit wav
    for audio_path in here().glob("data/audio/files_to_read/*.wav"):
        logger.info(f"Starting {audio_path=}")
        converted_audio_path = split_and_convert_audio(audio_path)
        logger.info(f"Converted to mono at {converted_audio_path=}")

        gcs_out_uri = f"gs://gen-ai-test-playground/audio-files-marketing/{converted_audio_path.name}"
        logger.info(f"Beginning upload to {gcs_out_uri=}")
        upload_to_gcs(converted_audio_path, gcs_out_uri)
        logger.info(f"Uploaded to {gcs_out_uri=}")

        logger.info(f"Beginning transcription of {gcs_out_uri}")
        transcript, _ = transcribe_gcs(gcs_out_uri)
        logger.info(f"Transcription complete {transcript[:500]=} {transcript[500:]=}")
        logger.info(f"Words: {len(transcript.split())=}")

        out_path = (
            here()
            / audio_path.parent.parent
            / "outs"
            / (audio_path.stem + "_transcript.txt")
        )
        logger.info(f"Writing to outpath: {out_path=}")
        with open(
            out_path,
            "w",
        ) as f:
            f.write(transcript)
            f.close()
    #
    # audio_path = here() / "data/audio/S6T01.wav"
    # converted_audio_path = split_and_convert_audio(audio_path)
    #
    # gcs_out_uri = (
    #     f"gs://gen-ai-test-playground/audio-files-marketing/{converted_audio_path.name}"
    # )
    # upload_to_gcs(converted_audio_path, gcs_out_uri)
    #
    # transcript, _ = transcribe_gcs(gcs_out_uri)
    # logger.info(transcript)
    #
    # # write the transcript to a file
    # with open(
    #     here() / audio_path.parent / (audio_path.stem + "_transcript.txt"), "w"
    # ) as f:
    #     f.write(transcript)
    #     f.close()
