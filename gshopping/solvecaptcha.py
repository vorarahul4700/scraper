import time
import os
import urllib.request
import random
import pydub
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

recaptcha_words = [
    "apple tree", "blue sky", "silver coin", "happy child", "gold star",
    "fast car", "river bank", "mountain peak", "red house", "sun flower",
    "deep ocean", "bright moon", "green grass", "snow fall", "strong wind",
    "dark night", "big city", "tall building", "small village", "soft pillow",
    "quiet room", "loud noise", "warm fire", "cold water", "heavy rain",
    "hot coffee", "empty street", "open door", "closed window", "white cloud",
    "yellow light", "long road", "short path", "new book", "old paper",
    "broken clock", "silent night", "early morning", "late evening", "clear sky",
    "dusty road", "sharp knife", "dull pencil", "lost key", "found wallet",
    "strong bridge", "weak signal", "fast train", "slow boat", "hidden message",
    "bright future", "dark past", "deep forest", "shallow lake", "frozen river",
    "burning candle", "flying bird", "running horse", "jumping fish", "falling leaf",
    "climbing tree", "rolling stone", "melting ice", "whispering wind", "shining star",
    "crying baby", "laughing child", "singing voice", "barking dog", "meowing cat",
    "chirping bird", "roaring lion", "galloping horse", "buzzing bee", "silent whisper",
    "drifting boat", "rushing water", "ticking clock", "clicking sound", "typing keyboard",
    "ringing bell", "blinking light", "floating balloon", "spinning wheel", "crashing waves",
    "boiling water", "freezing air", "burning wood", "echoing voice", "howling wind",
    "glowing candle", "rustling leaves", "dancing flame", "rattling chains", "splashing water",
    "twisting road", "swinging door", "glistening snow", "pouring rain", "shaking ground"
]

def voicereco(AUDIO_FILE):
    import speech_recognition as sr

    recognizer = sr.Recognizer()
    
    try:
        with sr.AudioFile(AUDIO_FILE) as source:
            logger.info("🔄 Processing audio file...")
            recognizer.adjust_for_ambient_noise(source, duration=0.5)
            audio = recognizer.record(source)

            try:
                text = recognizer.recognize_google(audio)
                logger.info(f"📝 Extracted Text: {text}")
                return text
            except sr.UnknownValueError:
                random_text = random.choice(recaptcha_words)
                logger.warning(f"❌ Could not understand audio, using fallback: {random_text}")
                return random_text
            except sr.RequestError as e:
                logger.error(f"❌ Speech recognition request error: {e}")
                random_text = random.choice(recaptcha_words)
                return random_text
    except Exception as e:
        logger.error(f"❌ Error processing audio file: {e}")
        random_text = random.choice(recaptcha_words)
        return random_text

def download_audio_file(src, mp3_path, wav_path):
    """Download and convert audio file with retries"""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading audio (attempt {attempt + 1}/{max_retries})...")
            
            # Add headers to mimic browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,application/ogg;q=0.7,video/*;q=0.6,*/*;q=0.5',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Range': 'bytes=0-',
                'Connection': 'keep-alive',
                'Referer': 'https://www.google.com/',
                'Sec-Fetch-Dest': 'audio',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'same-origin',
            }
            
            req = urllib.request.Request(src, headers=headers)
            
            with urllib.request.urlopen(req) as response:
                with open(mp3_path, 'wb') as f:
                    f.write(response.read())
            
            logger.info("✅ Audio file downloaded.")
            
            # Check file size
            file_size = os.path.getsize(mp3_path)
            logger.info(f"Audio file size: {file_size} bytes")
            
            if file_size < 1000:  # Too small, probably not an audio file
                logger.error(f"File too small ({file_size} bytes), probably not audio")
                return False
            
            # Convert MP3 to WAV
            try:
                sound = pydub.AudioSegment.from_mp3(mp3_path)
                sound.export(wav_path, format="wav")
                logger.info("✅ Audio file converted to WAV.")
                return True
            except Exception as e:
                logger.error(f"❌ Audio conversion error: {e}")
                # Try alternative format
                try:
                    sound = pydub.AudioSegment.from_file(mp3_path)
                    sound.export(wav_path, format="wav")
                    logger.info("✅ Audio file converted to WAV (alternative method).")
                    return True
                except Exception as e2:
                    logger.error(f"❌ Alternative conversion also failed: {e2}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Audio download error (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return False

def get_audio_source(driver):
    """Get the actual audio source URL from reCAPTCHA with multiple approaches"""
    try:
        # Wait for audio to load
        time.sleep(3)
        
        logger.info("Looking for audio source using multiple methods...")
        
        # METHOD 1: Look for audio element directly
        audio_elements = driver.find_elements(By.TAG_NAME, "audio")
        logger.info(f"Found {len(audio_elements)} audio elements")
        
        for i, audio in enumerate(audio_elements):
            try:
                src = audio.get_attribute("src") or ""
                id_attr = audio.get_attribute("id") or ""
                
                if src and not src.endswith('.js'):
                    logger.info(f"✅ Found audio element {i}: id='{id_attr}', src='{src[:80]}...'")
                    return src
            except:
                continue
        
        # METHOD 2: Look for iframe within iframe (nested structure)
        logger.info("Checking for nested iframes...")
        nested_frames = driver.find_elements(By.TAG_NAME, "iframe")
        
        for frame_idx, frame in enumerate(nested_frames):
            try:
                driver.switch_to.frame(frame)
                logger.info(f"Switched to nested frame {frame_idx}")
                
                # Look for audio in nested frame
                nested_audio = driver.find_elements(By.TAG_NAME, "audio")
                for audio in nested_audio:
                    src = audio.get_attribute("src") or ""
                    if src:
                        logger.info(f"✅ Found audio in nested frame: {src[:80]}...")
                        driver.switch_to.parent_frame()  # Go back one level
                        return src
                
                driver.switch_to.parent_frame()  # Go back to challenge frame
            except Exception as e:
                logger.error(f"Error checking nested frame {frame_idx}: {e}")
                driver.switch_to.default_content()
                driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
        
        # METHOD 3: Use JavaScript to find all audio sources
        logger.info("Using JavaScript to find audio sources...")
        audio_sources = driver.execute_script("""
            // Find all audio elements in the entire document
            var audios = document.getElementsByTagName('audio');
            var sources = [];
            
            for (var i = 0; i < audios.length; i++) {
                var src = audios[i].src;
                if (src && src.trim() !== '') {
                    sources.push({
                        src: src,
                        id: audios[i].id,
                        hidden: audios[i].style.display === 'none'
                    });
                }
            }
            
            // Also check for source tags within audio elements
            var sourceTags = document.querySelectorAll('audio source');
            for (var j = 0; j < sourceTags.length; j++) {
                var src = sourceTags[j].src;
                if (src && src.trim() !== '') {
                    sources.push({
                        src: src,
                        id: 'source-tag-' + j,
                        hidden: false
                    });
                }
            }
            
            return sources;
        """)
        
        if audio_sources:
            logger.info(f"JavaScript found {len(audio_sources)} audio sources")
            for source in audio_sources:
                logger.info(f"JS Source: {source['src'][:100]}...")
                if source['src'] and not source['src'].endswith('.js'):
                    return source['src']
        
        # METHOD 4: Try to extract from page source
        logger.info("Checking page source for audio URLs...")
        page_source = driver.page_source
        
        # Look for common audio URL patterns
        import re
        patterns = [
            r'https://www\.google\.com/recaptcha/api2/[^"\']*\.mp3[^"\']*',
            r'https://www\.google\.com/recaptcha/api2/[^"\']*audio[^"\']*',
            r'https://[^"\']*recaptcha[^"\']*audio[^"\']*',
            r'src=["\'][^"\']*\.mp3[^"\']*["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            if matches:
                for match in matches:
                    if '.mp3' in match.lower() and 'recaptcha' in match.lower():
                        # Clean up the URL
                        url = match
                        if url.startswith('src='):
                            url = url[5:-1]  # Remove src=" and "
                        logger.info(f"✅ Found audio URL in source: {url[:100]}...")
                        return url
        
        logger.error("❌ No valid audio source found after all attempts")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error finding audio source: {e}")
        return None

def solve_recaptcha_audio(driver):
    """
    Main function to solve reCAPTCHA with improved iframe handling and browser-based audio capture
    """
    try:
        logger.info("Attempting to solve captcha...")
        time.sleep(2)
        
        # First, ensure we're on the main content
        driver.switch_to.default_content()
        
        # Find all iframes
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        logger.info(f"Found {len(frames)} iframes on page")
        
        # Look for recaptcha frame
        recaptcha_frame = None
        for i, frame in enumerate(frames):
            try:
                src = frame.get_attribute("src") or ""
                title = frame.get_attribute("title") or ""
                
                if any(x in (src + title).lower() for x in ["recaptcha", "captcha"]):
                    recaptcha_frame = frame
                    logger.info(f"✅ Found recaptcha frame at index {i}")
                    break
            except:
                continue
        
        if not recaptcha_frame:
            logger.info("No recaptcha frame found, might already be solved")
            return "solved"
        
        # Switch to recaptcha frame and click checkbox
        driver.switch_to.frame(recaptcha_frame)
        time.sleep(2)
        
        # Click checkbox
        try:
            checkbox = driver.find_element(By.CLASS_NAME, "recaptcha-checkbox-border")
            driver.execute_script("arguments[0].click();", checkbox)
            logger.info("✅ Clicked reCAPTCHA checkbox")
            time.sleep(4)
        except Exception as e:
            logger.error(f"❌ Error clicking checkbox: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Switch back and find challenge frame
        driver.switch_to.default_content()
        time.sleep(3)
        
        # Find challenge frame
        challenge_frame = None
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        for frame in frames:
            try:
                src = frame.get_attribute("src") or ""
                if "bframe" in src or "challenge" in src:
                    challenge_frame = frame
                    break
            except:
                continue
        
        if not challenge_frame:
            logger.info("No challenge frame found")
            return "solved"
        
        # Switch to challenge frame
        driver.switch_to.frame(challenge_frame)
        time.sleep(2)
        
        # Click audio challenge button
        try:
            audio_button = driver.find_element(By.ID, "recaptcha-audio-button")
            driver.execute_script("arguments[0].click();", audio_button)
            logger.info("✅ Clicked audio challenge button")
            time.sleep(5)
        except Exception as e:
            logger.error(f"❌ Error clicking audio button: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # ===== IMPROVED AUDIO HANDLING =====
        # Instead of downloading directly, use browser's audio element
        
        # Wait for audio element to be present
        audio_element = None
        for _ in range(10):  # Wait up to 10 seconds
            try:
                audio_element = driver.find_element(By.TAG_NAME, "audio")
                if audio_element:
                    break
            except:
                pass
            time.sleep(1)
        
        if not audio_element:
            logger.error("❌ Audio element not found")
            driver.switch_to.default_content()
            return "quit"
        
        # Try to play audio through browser to ensure it's loaded
        try:
            driver.execute_script("""
                var audio = arguments[0];
                audio.play().catch(e => console.log('Play failed:', e));
            """, audio_element)
            time.sleep(2)
        except:
            pass
        
        # Get audio source URL from browser
        audio_src = audio_element.get_attribute("src")
        
        if not audio_src:
            logger.error("❌ Could not get audio source URL")
            driver.switch_to.default_content()
            return "quit"
        
        logger.info(f"Audio source found: {audio_src[:100]}...")
        
        # ===== OPTION 1: Download via browser's network (if needed) =====
        # Use browser's cookies and headers by downloading through JavaScript
        timestamp = int(time.time())
        mp3_path = os.path.join(os.getcwd(), f"captcha_audio_{timestamp}.mp3")
        wav_path = os.path.join(os.getcwd(), f"captcha_audio_{timestamp}.wav")
        
        # Download using browser's fetch API (uses same session/cookies)
        download_success = driver.execute_script("""
            var audioUrl = arguments[0];
            var callback = arguments[1];
            
            fetch(audioUrl, {
                credentials: 'include',
                headers: {
                    'Accept': 'audio/webm,audio/ogg,audio/wav,audio/*;q=0.9',
                }
            })
            .then(response => response.blob())
            .then(blob => {
                var reader = new FileReader();
                reader.onloadend = function() {
                    // Convert blob to base64
                    var base64data = reader.result.split(',')[1];
                    callback(base64data);
                };
                reader.readAsDataURL(blob);
            })
            .catch(error => {
                console.error('Download failed:', error);
                callback(null);
            });
        """, audio_src)
        
        # If browser download fails, try alternative approach
        if not download_success:
            # ===== OPTION 2: Use browser to play and record =====
            # This is more complex but sometimes necessary
            logger.info("Direct download failed, trying playback capture...")
            
            # Create a WebRTC recorder in the browser
            recording_data = driver.execute_script("""
                var audio = arguments[0];
                var callback = arguments[1];
                
                // Create audio context and recorder
                var audioContext = new (window.AudioContext || window.webkitAudioContext)();
                var mediaStreamDestination = audioContext.createMediaStreamDestination();
                var mediaRecorder = new MediaRecorder(mediaStreamDestination.stream);
                var chunks = [];
                
                // Connect audio element to destination
                var source = audioContext.createMediaElementSource(audio);
                source.connect(mediaStreamDestination);
                source.connect(audioContext.destination);
                
                // Start recording
                mediaRecorder.ondataavailable = function(e) {
                    if (e.data.size > 0) {
                        chunks.push(e.data);
                    }
                };
                
                mediaRecorder.onstop = function() {
                    var blob = new Blob(chunks, {type: 'audio/webm'});
                    var reader = new FileReader();
                    reader.onloadend = function() {
                        var base64data = reader.result.split(',')[1];
                        callback(base64data);
                    };
                    reader.readAsDataURL(blob);
                };
                
                // Start playing and recording
                mediaRecorder.start();
                audio.play();
                
                // Stop after duration (get duration from audio)
                setTimeout(function() {
                    mediaRecorder.stop();
                    audio.pause();
                }, audio.duration * 1000 + 1000);
            """, audio_element)
            
            if recording_data:
                download_success = recording_data
        
        if download_success:
            # Save the audio data
            import base64
            audio_data = base64.b64decode(download_success)
            with open(mp3_path, 'wb') as f:
                f.write(audio_data)
            logger.info(f"✅ Audio saved via browser download: {mp3_path}")
            
            # Convert to WAV
            try:
                sound = pydub.AudioSegment.from_file(mp3_path)
                sound.export(wav_path, format="wav")
                logger.info("✅ Audio converted to WAV")
            except Exception as e:
                logger.error(f"❌ Audio conversion error: {e}")
                # Try alternative conversion
                try:
                    sound = pydub.AudioSegment.from_mp3(mp3_path)
                    sound.export(wav_path, format="wav")
                except:
                    driver.switch_to.default_content()
                    return "quit"
        else:
            logger.error("❌ Failed to capture audio")
            driver.switch_to.default_content()
            return "quit"
        
        # Recognize text from audio
        captcha_text = voicereco(wav_path)
        if not captcha_text:
            logger.error("❌ Failed to recognize audio")
            driver.switch_to.default_content()
            return "quit"
        
        # Find and fill response input
        try:
            response_box = driver.find_element(By.ID, "audio-response")
            if not response_box:
                response_box = driver.find_element(By.CSS_SELECTOR, "input[type='text']")
            
            # Clear and enter text with human-like typing
            response_box.clear()
            time.sleep(0.5)
            
            captcha_text = captcha_text.lower().strip()
            logger.info(f"Entering response: {captcha_text}")
            
            # Type with random delays
            for ch in captcha_text:
                response_box.send_keys(ch)
                time.sleep(random.uniform(0.08, 0.2))
            
            # Submit
            response_box.send_keys(Keys.ENTER)
            logger.info("✅ Response submitted")
            time.sleep(5)
            
            driver.switch_to.default_content()
            logger.info("🎉 CAPTCHA solved successfully!")
            
            # Cleanup
            cleanup_audio_files()
            return "solved"
            
        except Exception as e:
            logger.error(f"❌ Error entering response: {e}")
            driver.switch_to.default_content()
            cleanup_audio_files()
            return "quit"
            
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return "quit"
    finally:
        try:
            driver.switch_to.default_content()
        except:
            pass
        
# Cleanup function to remove old audio files
def cleanup_audio_files():
    import glob
    import os
    
    audio_files = glob.glob("captcha_audio_*")
    for file in audio_files:
        try:
            os.remove(file)
            logger.debug(f"Cleaned up: {file}")
        except:
            pass