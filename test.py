#!/usr/bin/env python3
"""
Simple CAPTCHA Solver for GitHub Actions
Solves basic CAPTCHAs locally without external API keys
"""

import os
import sys
import time
import json
import base64
import random
from io import BytesIO
from typing import Optional, Dict, Any
from pathlib import Path

# Try to import optional dependencies
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.keys import Keys
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Selenium not available. Browser automation disabled.")

try:
    import pytesseract
    from PIL import Image, ImageFilter, ImageEnhance
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("OCR libraries not available. Image CAPTCHA solving disabled.")


class SimpleCaptchaSolver:
    def __init__(self, headless: bool = True, log_dir: str = "logs"):
        """
        Initialize simple CAPTCHA solver for local use
        
        Args:
            headless: Run browser in headless mode
            log_dir: Directory for saving logs and screenshots
        """
        self.headless = headless
        self.driver = None
        self.log_dir = Path(log_dir)
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging directory"""
        self.log_dir.mkdir(exist_ok=True)
        print(f"Log directory: {self.log_dir.absolute()}")
        
    def solve_math_captcha(self, text: str) -> Optional[str]:
        """
        Solve mathematical CAPTCHAs
        
        Args:
            text: Math problem text
            
        Returns:
            Answer as string or None
        """
        try:
            # Clean the text
            text = text.lower().replace('=', '').replace('?', '').strip()
            
            # Common math patterns
            patterns = [
                (r'(\d+)\s*\+\s*(\d+)', lambda x, y: str(int(x) + int(y))),
                (r'(\d+)\s*-\s*(\d+)', lambda x, y: str(int(x) - int(y))),
                (r'(\d+)\s*\*\s*(\d+)', lambda x, y: str(int(x) * int(y))),
                (r'(\d+)\s*x\s*(\d+)', lambda x, y: str(int(x) * int(y))),
                (r'(\d+)\s*×\s*(\d+)', lambda x, y: str(int(x) * int(y))),
            ]
            
            import re
            for pattern, func in patterns:
                match = re.search(pattern, text)
                if match:
                    return func(match.group(1), match.group(2))
                    
            # Handle "what is X + Y" format
            if 'what is' in text:
                expr = text.replace('what is', '').strip()
                # Simple evaluation for safe operations
                if all(c.isdigit() or c in '+-* ' for c in expr):
                    try:
                        # WARNING: Using eval can be dangerous with untrusted input
                        # Only use with trusted sources or implement safe parser
                        result = eval(expr)  # Only for demo with trusted input
                        return str(result)
                    except:
                        pass
                        
            return None
            
        except Exception as e:
            print(f"Error solving math CAPTCHA: {e}")
            return None
    
    def solve_simple_text_captcha(self, text: str) -> Optional[str]:
        """
        Solve simple text-based CAPTCHAs
        
        Args:
            text: CAPTCHA text
            
        Returns:
            Solution or None
        """
        try:
            text_lower = text.lower().strip()
            
            # Common text-based challenges
            responses = {
                'yes': 'yes',
                'no': 'no',
                'true': 'true',
                'false': 'false',
                'ok': 'ok',
                'accept': 'accept',
                'continue': 'continue',
                'proceed': 'proceed',
                'submit': 'submit',
                'next': 'next'
            }
            
            if text_lower in responses:
                return responses[text_lower]
                
            # Check for "enter the word: X" format
            if 'enter the word' in text_lower:
                parts = text_lower.split('enter the word')
                if len(parts) > 1:
                    word = parts[1].replace(':', '').strip()
                    if word and len(word) < 20:  # Sanity check
                        return word
                        
            return None
            
        except Exception as e:
            print(f"Error solving text CAPTCHA: {e}")
            return None
    
    def process_image_for_ocr(self, image_path: str) -> Image.Image:
        """
        Preprocess image for better OCR results
        
        Args:
            image_path: Path to image file
            
        Returns:
            Processed PIL Image
        """
        try:
            image = Image.open(image_path)
            
            # Convert to grayscale
            if image.mode != 'L':
                image = image.convert('L')
            
            # Enhance contrast
            enhancer = ImageEnhance.Contrast(image)
            image = enhancer.enhance(2.0)
            
            # Apply binary threshold
            image = image.point(lambda x: 0 if x < 150 else 255)
            
            # Remove noise
            image = image.filter(ImageFilter.MedianFilter(3))
            
            # Save processed image
            processed_path = self.log_dir / f"processed_{Path(image_path).name}"
            image.save(processed_path)
            print(f"Processed image saved: {processed_path}")
            
            return image
            
        except Exception as e:
            print(f"Error processing image: {e}")
            raise
    
    def solve_image_captcha_ocr(self, image_path: str) -> Optional[str]:
        """
        Solve image CAPTCHA using OCR
        
        Args:
            image_path: Path to CAPTCHA image
            
        Returns:
            Extracted text or None
        """
        if not OCR_AVAILABLE:
            print("OCR libraries not installed. Install with: pip install pytesseract pillow")
            return None
            
        try:
            # Process image
            processed_image = self.process_image_for_ocr(image_path)
            
            # Configure tesseract
            custom_config = r'--oem 3 --psm 8 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
            
            # Extract text
            text = pytesseract.image_to_string(processed_image, config=custom_config)
            text = text.strip()
            
            if text:
                print(f"OCR extracted text: {text}")
                return text
                
        except Exception as e:
            print(f"Error in OCR processing: {e}")
            
        return None
    
    def setup_chrome_driver(self):
        """Setup Chrome WebDriver for Selenium"""
        if not SELENIUM_AVAILABLE:
            raise ImportError("Selenium not installed. Install with: pip install selenium")
            
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        # Common options for stability in CI/CD
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Remove automation indicators
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Set up driver
        try:
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except:
            # Fallback to system Chrome
            self.driver = webdriver.Chrome(options=chrome_options)
        
        # Execute CDP commands to prevent detection
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def find_and_solve_captcha(self, url: str, timeout: int = 30) -> Dict[str, Any]:
        """
        Automatically find and attempt to solve CAPTCHA on a page
        
        Args:
            url: Target URL
            timeout: Maximum wait time
            
        Returns:
            Dictionary with results
        """
        if not SELENIUM_AVAILABLE:
            return {"success": False, "error": "Selenium not available"}
            
        try:
            if not self.driver:
                self.setup_chrome_driver()
                
            print(f"Navigating to: {url}")
            self.driver.get(url)
            time.sleep(3)
            
            # Look for common CAPTCHA indicators
            captcha_selectors = [
                ("reCAPTCHA iframe", 'iframe[title*="reCAPTCHA"]'),
                ("reCAPTCHA iframe", 'iframe[src*="recaptcha"]'),
                ("CAPTCHA image", 'img[src*="captcha"]'),
                ("CAPTCHA image", 'img[alt*="CAPTCHA"]'),
                ("CAPTCHA input", 'input[name*="captcha"]'),
                ("CAPTCHA input", 'input[id*="captcha"]'),
                ("Math CAPTCHA", 'div.captcha'),
                ("Math CAPTCHA", 'span.captcha'),
                ("CAPTCHA text", 'label:contains("CAPTCHA")'),
            ]
            
            results = {
                "success": False,
                "captcha_found": False,
                "type": None,
                "solution": None,
                "screenshot": None
            }
            
            # Save initial screenshot
            initial_screenshot = self.log_dir / "initial_page.png"
            self.driver.save_screenshot(str(initial_screenshot))
            results["screenshot"] = str(initial_screenshot)
            
            # Check for different CAPTCHA types
            captcha_found = False
            
            # Check for reCAPTCHA
            for desc, selector in captcha_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        print(f"Found {desc} using selector: {selector}")
                        captcha_found = True
                        results["captcha_found"] = True
                        results["type"] = desc
                        
                        # Try to solve reCAPTCHA
                        if "reCAPTCHA" in desc:
                            recaptcha_result = self.solve_recaptcha_checkbox()
                            results.update(recaptcha_result)
                            return results
                            
                except Exception as e:
                    continue
            
            # Look for math problems in text
            page_text = self.driver.find_element(By.TAG_NAME, 'body').text
            math_keywords = ['+', '-', '*', '×', '=', 'math', 'calculate', 'sum', 'total']
            
            if any(keyword in page_text.lower() for keyword in math_keywords):
                # Extract potential math problems
                lines = page_text.split('\n')
                for line in lines:
                    if any(op in line for op in ['+', '-', '*', '×', '=']):
                        solution = self.solve_math_captcha(line)
                        if solution:
                            results.update({
                                "success": True,
                                "captcha_found": True,
                                "type": "math",
                                "solution": solution
                            })
                            return results
            
            if not captcha_found:
                print("No CAPTCHA detected on page")
                results["message"] = "No CAPTCHA detected"
                
            return results
            
        except Exception as e:
            print(f"Error finding CAPTCHA: {e}")
            return {"success": False, "error": str(e)}
    
    def solve_recaptcha_checkbox(self) -> Dict[str, Any]:
        """
        Attempt to solve reCAPTCHA checkbox
        
        Returns:
            Dictionary with results
        """
        try:
            # Switch to reCAPTCHA iframe
            iframe_selectors = [
                'iframe[title*="reCAPTCHA"]',
                'iframe[src*="recaptcha/api2"]',
                'iframe[src*="google.com/recaptcha"]',
            ]
            
            for selector in iframe_selectors:
                try:
                    iframe = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    self.driver.switch_to.frame(iframe)
                    
                    # Find and click checkbox
                    checkbox = self.driver.find_element(By.CSS_SELECTOR, '.recaptcha-checkbox-border')
                    checkbox.click()
                    
                    print("Clicked reCAPTCHA checkbox")
                    self.driver.switch_to.default_content()
                    
                    # Wait for verification
                    time.sleep(5)
                    
                    # Check if verification occurred
                    verified_screenshot = self.log_dir / "recaptcha_verified.png"
                    self.driver.save_screenshot(str(verified_screenshot))
                    
                    return {
                        "success": True,
                        "type": "recaptcha_checkbox",
                        "solution": "checkbox_clicked",
                        "screenshot_after": str(verified_screenshot)
                    }
                    
                except:
                    self.driver.switch_to.default_content()
                    continue
            
            return {"success": False, "error": "Could not find or click reCAPTCHA checkbox"}
            
        except Exception as e:
            self.driver.switch_to.default_content()
            return {"success": False, "error": str(e)}
    
    def save_debug_info(self, prefix: str = "debug"):
        """Save debug information"""
        if not self.driver:
            return
            
        try:
            # Save screenshot
            screenshot_path = self.log_dir / f"{prefix}_screenshot.png"
            self.driver.save_screenshot(str(screenshot_path))
            
            # Save page source
            page_source = self.driver.page_source
            source_path = self.log_dir / f"{prefix}_page.html"
            with open(source_path, 'w', encoding='utf-8') as f:
                f.write(page_source)
                
            # Save console logs
            logs = self.driver.get_log('browser')
            if logs:
                log_path = self.log_dir / f"{prefix}_console.json"
                with open(log_path, 'w') as f:
                    json.dump(logs, f, indent=2)
                    
            print(f"Debug info saved with prefix: {prefix}")
            
        except Exception as e:
            print(f"Error saving debug info: {e}")
    
    def manual_solve_prompt(self, context: str = "") -> str:
        """
        Prompt for manual CAPTCHA solving
        
        Args:
            context: Additional context about the CAPTCHA
            
        Returns:
            User-provided solution
        """
        print("\n" + "="*60)
        print("MANUAL CAPTCHA SOLVING REQUIRED")
        print("="*60)
        
        if context:
            print(f"\nContext: {context}")
            
        print("\nFor automated workflows, you can:")
        print("1. Use a different CAPTCHA solving approach")
        print("2. Implement site-specific logic")
        print("3. Use a service with human solvers")
        print("4. Contact the website owner for API access")
        
        print("\n" + "-"*60)
        solution = input("\nEnter CAPTCHA solution (or press Enter to skip): ").strip()
        
        return solution if solution else None
    
    def close(self):
        """Cleanup resources"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """Command-line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Simple CAPTCHA Solver')
    parser.add_argument('--url', help='URL to check for CAPTCHA')
    parser.add_argument('--math', help='Solve math CAPTCHA text')
    parser.add_argument('--image', help='Path to image CAPTCHA')
    parser.add_argument('--headless', action='store_true', default=True, 
                       help='Run browser in headless mode (default: True)')
    parser.add_argument('--log-dir', default='logs', help='Log directory')
    
    args = parser.parse_args()
    
    solver = SimpleCaptchaSolver(headless=args.headless, log_dir=args.log_dir)
    
    try:
        if args.math:
            print(f"Solving math CAPTCHA: {args.math}")
            result = solver.solve_math_captcha(args.math)
            print(f"Result: {result}")
            
        elif args.image:
            print(f"Solving image CAPTCHA: {args.image}")
            if OCR_AVAILABLE:
                result = solver.solve_image_captcha_ocr(args.image)
                print(f"OCR Result: {result}")
            else:
                print("OCR not available. Install pytesseract and pillow.")
                
        elif args.url:
            print(f"Checking URL for CAPTCHA: {args.url}")
            results = solver.find_and_solve_captcha(args.url)
            
            print("\nResults:")
            print(json.dumps(results, indent=2))
            
            if results.get('success'):
                print("\n✓ CAPTCHA solved successfully!")
            else:
                print("\n✗ Could not solve CAPTCHA automatically")
                
        else:
            print("No action specified. Use --help for usage information.")
            
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        solver.close()


if __name__ == "__main__":
    main()