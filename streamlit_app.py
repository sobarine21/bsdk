import streamlit as st
import assemblyai as aai
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import google.generativeai as genai
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import langid
from collections import Counter
import os
import time

# Download necessary corpora for NLTK
nltk.download('vader_lexicon')

# Set up AssemblyAI API key
aai.settings.api_key = st.secrets["ASSEMBLY_API_KEY"]  # Set your API key here

# Function to send the audio file to AssemblyAI API for transcription
def transcribe_audio(file):
    try:
        # Create a Transcriber instance
        transcriber = aai.Transcriber()
        
        # If the file is uploaded, save it temporarily
        with open("temp_audio_file", "wb") as f:
            f.write(file.read())
        
        # Transcribe the local file
        transcript = transcriber.transcribe("temp_audio_file")
        
        # Check for errors and return result
        if transcript.status == aai.TranscriptStatus.error:
            return {"error": transcript.error}
        else:
            return {"text": transcript.text}  # Return transcription text

    except Exception as e:
        return {"error": str(e)}

# Enhanced sentiment analysis with VADER
def analyze_vader_sentiment(text):
    sia = SentimentIntensityAnalyzer()
    sentiment = sia.polarity_scores(text)
    return sentiment

# Function for keyword extraction using CountVectorizer (no NLTK needed)
def extract_keywords(text):
    vectorizer = CountVectorizer(stop_words='english', max_features=10)  # Extract top 10 frequent words
    X = vectorizer.fit_transform([text])
    keywords = vectorizer.get_feature_names_out()
    return keywords

# Function to simulate speaker detection (based on pauses in speech, for now, this is a placeholder)
def detect_speakers(audio_file):
    audio_data = audio_file.read()
    duration = len(audio_data) / (44100 * 2)  # Assuming 44.1kHz sample rate and 16-bit samples
    segments = int(duration // 2)  # Simulate speaker detection by splitting into 2-second intervals
    speakers = [f"Speaker {i+1}: {2*i} - {2*(i+1)} seconds" for i in range(segments)]
    return speakers

# Function to calculate speech rate (words per minute)
def calculate_speech_rate(text, duration_seconds):
    words = text.split()
    num_words = len(words)
    if duration_seconds > 0:
        speech_rate = num_words / (duration_seconds / 60)
    else:
        speech_rate = 0
    return speech_rate

# Function to calculate pause duration (simulated)
def calculate_pause_duration(audio_file):
    audio_data = audio_file.read()
    duration = len(audio_data) / (44100 * 2)  # Assuming 44.1kHz sample rate and 16-bit samples
    pause_duration = duration * 0.1  # Simulate 10% of the duration as pauses
    return pause_duration

# Function to analyze call sentiment over time (simulated)
def analyze_sentiment_over_time(text):
    sentences = text.split('.')
    sentiment_over_time = [analyze_vader_sentiment(sentence)['compound'] for sentence in sentences if sentence]
    return sentiment_over_time

# Detect language of the text
def detect_language(text):
    lang, confidence = langid.classify(text)
    return lang, confidence

# Function to calculate word frequency (top 20 most frequent words)
def word_frequency(text):
    words = text.split()
    word_counts = Counter(words)
    most_common_words = word_counts.most_common(20)
    return most_common_words

# Function to generate a word cloud
def generate_word_cloud(text):
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
    return wordcloud

# Streamlit UI
st.title("🎙️ Audio Transcription & Analysis Web App")
st.write("Upload an audio file, and this app will transcribe it using AssemblyAI.")

# File uploader
uploaded_file = st.file_uploader("Upload your audio file (e.g., .wav, .flac, .mp3)", type=["wav", "flac", "mp3"])

if uploaded_file is not None:
    # Display uploaded audio
    st.audio(uploaded_file, format="audio/mp3", start_time=0)
    st.info("Transcribing audio... Please wait.")
    
    # Transcribe the uploaded audio file
    result = transcribe_audio(uploaded_file)
    
    # Display the result
    if "text" in result:
        st.success("Transcription Complete:")
        transcription_text = result["text"]
        st.write(transcription_text)
        
        # Sentiment Analysis (VADER)
        vader_sentiment = analyze_vader_sentiment(transcription_text)
        st.subheader("Sentiment Analysis (VADER)")
        st.write(f"Positive: {vader_sentiment['pos']}, Neutral: {vader_sentiment['neu']}, Negative: {vader_sentiment['neg']}")

        # Language Detection
        lang, confidence = detect_language(transcription_text)
        st.subheader("Language Detection")
        st.write(f"Detected Language: {lang}, Confidence: {confidence}")

        # Keyword Extraction
        keywords = extract_keywords(transcription_text)
        st.subheader("Keyword Extraction")
        st.write(keywords)

        # Speaker Detection (placeholder for actual implementation)
        speakers = detect_speakers(uploaded_file)
        st.subheader("Speaker Detection (Placeholder)")
        st.write(speakers)

        # Speech Rate Calculation
        try:
            duration_seconds = len(uploaded_file.read()) / (44100 * 2)  # Assuming 44.1kHz sample rate and 16-bit samples
            speech_rate = calculate_speech_rate(transcription_text, duration_seconds)
            st.subheader("Speech Rate")
            st.write(f"Speech Rate: {speech_rate} words per minute")
        except ZeroDivisionError:
            st.error("Error: The duration of the audio is zero, which caused a division by zero error.")

        # Pause Duration Calculation
        try:
            pause_duration = calculate_pause_duration(uploaded_file)
            st.subheader("Pause Duration")
            st.write(f"Total Pause Duration: {pause_duration} seconds")
        except ZeroDivisionError:
            st.error("Error: The duration of the audio is zero, which caused a division by zero error.")

        # Sentiment Analysis Over Time
        sentiment_over_time = analyze_sentiment_over_time(transcription_text)
        st.subheader("Sentiment Analysis Over Time")
        st.line_chart(sentiment_over_time)

        # Word Frequency Analysis
        word_freq = word_frequency(transcription_text)
        st.subheader("Word Frequency Analysis")
        st.write(word_freq)

        # Word Cloud Visualization
        wordcloud = generate_word_cloud(transcription_text)
        st.subheader("Word Cloud")
        st.image(wordcloud.to_array())

        # Plot sentiment distribution
        st.subheader("Sentiment Distribution")
        plt.hist(sentiment_over_time, bins=20, color='blue', alpha=0.7)
        plt.xlabel('Sentiment Polarity')
        plt.ylabel('Frequency')
        st.pyplot(plt.gcf())
        
        # Add download button for the transcription text
        st.download_button(
            label="Download Transcription",
            data=transcription_text,
            file_name="transcription.txt",
            mime="text/plain"
        )
        
        # Add download button for analysis results
        analysis_results = f"""
        Sentiment Analysis (VADER):
        Positive: {vader_sentiment['pos']}, Neutral: {vader_sentiment['neu']}, Negative: {vader_sentiment['neg']}
        
        Language Detection:
        Detected Language: {lang}, Confidence: {confidence}
        
        Keyword Extraction:
        {keywords}
        
        Speech Rate: {speech_rate} words per minute
        Total Pause Duration: {pause_duration} seconds
        """
        st.download_button(
            label="Download Analysis Results",
            data=analysis_results,
            file_name="analysis_results.txt",
            mime="text/plain"
        )

        # Generative AI Analysis
        st.subheader("Generative AI Analysis")
        prompt = f"Analyze the following call recording transcription for professional call audit purposes in precise way and focus on highlighting the support agents KPI & metrics give numbers and scores for the performance on the metrics : {transcription_text}"
        
        # Let user decide if they want to use AI analysis
        if st.button("Run AI Analysis"):
            try:
                # Load and configure the model
                model_name, GOOGLE_API_KEY = get_next_model_and_key()  # Use your existing function here
                model = genai.GenerativeModel(model_name)
                
                # Generate response from the model
                response = model.generate_content(prompt)
                
                # Display response in Streamlit
                st.write("AI Analysis Response:")
                st.write(response.text)
            except Exception as e:
                st.error(f"Error: {e}")

    elif "error" in result:
        st.error(f"Error: {result['error']}")
    else:
        st.warning("Unexpected response from the API.")
