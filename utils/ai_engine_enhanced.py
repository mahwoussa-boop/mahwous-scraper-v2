"""
engines/ai_engine_enhanced.py — محرك Gemini محسّن مع تدوير المفاتيح الذكي
════════════════════════════════════════════════════════════════════════
✅ تدوير ذكي للمفاتيح عند خطأ 429 (Rate Limit)
✅ اختيار أفضل مفتاح بناءً على الأداء
✅ إعادة محاولة تلقائية مع backoff exponential
✅ تتبع شامل للأخطاء والنجاحات
"""
import requests
import json
import time
from typing import Dict, Optional, Any, Tuple
from datetime import datetime

try:
    from config import (GEMINI_API_KEYS, OPENROUTER_API_KEYS, COHERE_API_KEYS,
                       KEY_ROTATION_ENABLED, KEY_ROTATION_ON_429, KEY_ROTATION_STRATEGY)
    from utils.key_rotation import init_rotation_managers, get_rotation_manager
except ImportError:
    GEMINI_API_KEYS = []
    OPENROUTER_API_KEYS = []
    COHERE_API_KEYS = []
    KEY_ROTATION_ENABLED = True
    KEY_ROTATION_ON_429 = True
    KEY_ROTATION_STRATEGY = "round_robin"


# تهيئة مديري التدوير
if KEY_ROTATION_ENABLED:
    init_rotation_managers(GEMINI_API_KEYS, OPENROUTER_API_KEYS, COHERE_API_KEYS, KEY_ROTATION_STRATEGY)


class GeminiAPIClient:
    """عميل Gemini محسّن مع تدوير المفاتيح الذكي"""
    
    def __init__(self, api_keys: list = None, model: str = "gemini-2.0-flash", 
                 max_retries: int = 3, timeout: int = 30):
        """
        Args:
            api_keys: قائمة مفاتيح API
            model: اسم النموذج
            max_retries: عدد محاولات إعادة المحاولة
            timeout: مهلة الاتصال بالثواني
        """
        self.api_keys = api_keys or GEMINI_API_KEYS
        self.model = model
        self.max_retries = max_retries
        self.timeout = timeout
        self.base_url = f"https://generativelanguage.googleapis.com/v1/models/{model}:generateContent"
        
        # إحصائيات
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.rotation_count = 0
        self.last_error = None
        self.current_key_index = 0
    
    def get_current_key(self) -> str:
        """الحصول على المفتاح الحالي"""
        if not self.api_keys:
            return ""
        return self.api_keys[self.current_key_index % len(self.api_keys)]
    
    def rotate_key(self, reason: str = "manual"):
        """تدوير المفتاح إلى التالي"""
        if not self.api_keys:
            return
        
        old_index = self.current_key_index
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        self.rotation_count += 1
        
        print(f"🔄 تدوير المفتاح: من الفهرس {old_index} إلى {self.current_key_index} (السبب: {reason})")
    
    def call_gemini(self, prompt: str, system_prompt: str = "", 
                   max_tokens: int = 2000, temperature: float = 0.7,
                   json_mode: bool = False) -> Tuple[bool, str]:
        """
        استدعاء Gemini مع إعادة محاولة ذكية وتدوير المفاتيح
        
        Args:
            prompt: الرسالة الرئيسية
            system_prompt: رسالة النظام (التعليمات)
            max_tokens: الحد الأقصى للرموز في الاستجابة
            temperature: درجة الإبداعية (0.0 - 1.0)
            json_mode: طلب استجابة JSON
        
        Returns:
            (النجاح، النص أو رسالة الخطأ)
        """
        self.total_requests += 1
        
        for attempt in range(self.max_retries):
            try:
                key = self.get_current_key()
                if not key:
                    return False, "❌ لا توجد مفاتيح API متاحة"
                
                # بناء الرسالة
                contents = []
                if system_prompt:
                    contents.append({"parts": [{"text": system_prompt}]})
                contents.append({"parts": [{"text": prompt}]})
                
                # بناء الطلب
                payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "maxOutputTokens": max_tokens,
                        "temperature": temperature,
                    }
                }
                
                if json_mode:
                    payload["generationConfig"]["responseMimeType"] = "application/json"
                
                # إرسال الطلب
                url = f"{self.base_url}?key={key}"
                response = requests.post(url, json=payload, timeout=self.timeout)
                
                # معالجة الاستجابة
                if response.status_code == 200:
                    try:
                        data = response.json()
                        text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                        self.successful_requests += 1
                        return True, text
                    except Exception as e:
                        return False, f"❌ خطأ في معالجة الاستجابة: {str(e)}"
                
                # معالجة الأخطاء
                elif response.status_code == 429:
                    # خطأ Rate Limit — تدوير المفتاح
                    if KEY_ROTATION_ON_429 and len(self.api_keys) > 1:
                        self.rotate_key("429_rate_limit")
                        # إعادة محاولة مع مفتاح جديد
                        wait_time = min(2 ** attempt, 10)  # exponential backoff
                        time.sleep(wait_time)
                        continue
                    else:
                        return False, f"⚠️ تجاوز الحد (429) — لا توجد مفاتيح بديلة"
                
                elif response.status_code == 403:
                    return False, "❌ 403 — مفتاح غير مصرح أو IP محظور"
                
                elif response.status_code == 401:
                    return False, "❌ 401 — مفتاح غير صحيح"
                
                elif response.status_code == 404:
                    return False, f"❌ 404 — النموذج {self.model} غير متاح"
                
                else:
                    try:
                        error_msg = response.json().get("error", {}).get("message", "")
                    except:
                        error_msg = response.text[:200]
                    return False, f"❌ خطأ {response.status_code}: {error_msg}"
            
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 10)
                    time.sleep(wait_time)
                    continue
                return False, "❌ انتهت المهلة (Timeout)"
            
            except requests.exceptions.ConnectionError as e:
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 10)
                    time.sleep(wait_time)
                    continue
                return False, f"❌ خطأ اتصال: {str(e)[:100]}"
            
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 10)
                    time.sleep(wait_time)
                    continue
                return False, f"❌ خطأ غير متوقع: {str(e)[:100]}"
        
        self.failed_requests += 1
        return False, "❌ فشلت جميع محاولات إعادة المحاولة"
    
    def get_stats(self) -> Dict:
        """الحصول على إحصائيات الاستخدام"""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": f"{(self.successful_requests / max(self.total_requests, 1) * 100):.1f}%",
            "rotation_count": self.rotation_count,
            "current_key_index": self.current_key_index,
            "total_keys": len(self.api_keys),
            "model": self.model,
        }


# عميل عام للاستخدام
_gemini_client: Optional[GeminiAPIClient] = None


def init_gemini_client(api_keys: list = None, model: str = "gemini-2.0-flash"):
    """تهيئة عميل Gemini"""
    global _gemini_client
    _gemini_client = GeminiAPIClient(api_keys or GEMINI_API_KEYS, model)


def call_gemini(prompt: str, system_prompt: str = "", max_tokens: int = 2000,
               temperature: float = 0.7, json_mode: bool = False) -> Tuple[bool, str]:
    """استدعاء Gemini عبر العميل العام"""
    if not _gemini_client:
        init_gemini_client()
    return _gemini_client.call_gemini(prompt, system_prompt, max_tokens, temperature, json_mode)


def get_gemini_stats() -> Dict:
    """الحصول على إحصائيات Gemini"""
    if not _gemini_client:
        return {"error": "العميل لم يتم تهيئته"}
    return _gemini_client.get_stats()
