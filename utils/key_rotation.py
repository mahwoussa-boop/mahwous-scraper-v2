"""
utils/key_rotation.py — نظام تدوير المفاتيح الذكي (Key Rotation System)
════════════════════════════════════════════════════════════════════════
✅ تبديل تلقائي للمفاتيح عند خطأ 429 (Rate Limit)
✅ استراتيجيات تدوير ذكية (Round Robin, Random)
✅ تتبع حالة المفاتيح والأخطاء
✅ دعم متعدد المزودين (Gemini, OpenRouter, Cohere)
"""
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict


class KeyRotationManager:
    """مدير تدوير المفاتيح الذكي"""
    
    def __init__(self, keys: List[str], provider: str = "gemini", strategy: str = "round_robin"):
        """
        Args:
            keys: قائمة المفاتيح المتاحة
            provider: اسم المزود (gemini, openrouter, cohere)
            strategy: استراتيجية التدوير (round_robin, random)
        """
        self.keys = [k for k in keys if k]  # تنظيف المفاتيح الفارغة
        self.provider = provider
        self.strategy = strategy
        self.current_index = 0
        
        # تتبع الأخطاء والحالة
        self.key_errors = defaultdict(int)  # عدد الأخطاء لكل مفتاح
        self.key_last_error = defaultdict(lambda: None)  # آخر خطأ لكل مفتاح
        self.key_429_count = defaultdict(int)  # عدد أخطاء 429 لكل مفتاح
        self.key_last_429 = defaultdict(lambda: None)  # آخر خطأ 429 لكل مفتاح
        self.key_success_count = defaultdict(int)  # عدد الطلبات الناجحة
        
        # إحصائيات عامة
        self.total_rotations = 0
        self.total_429_errors = 0
        self.rotation_history = []  # سجل التدويرات
    
    def get_current_key(self) -> str:
        """الحصول على المفتاح الحالي"""
        if not self.keys:
            return ""
        return self.keys[self.current_index % len(self.keys)]
    
    def rotate_key(self, reason: str = "manual") -> Tuple[str, int]:
        """
        تدوير المفتاح إلى التالي
        
        Args:
            reason: سبب التدوير (manual, 429_error, max_errors, etc.)
        
        Returns:
            (المفتاح الجديد، فهرس المفتاح الجديد)
        """
        if not self.keys:
            return "", -1
        
        old_key = self.get_current_key()
        old_index = self.current_index
        
        if self.strategy == "round_robin":
            self.current_index = (self.current_index + 1) % len(self.keys)
        elif self.strategy == "random":
            # اختيار مفتاح عشوائي مختلف عن الحالي
            available = [i for i in range(len(self.keys)) if i != self.current_index]
            if available:
                self.current_index = random.choice(available)
        
        new_key = self.get_current_key()
        self.total_rotations += 1
        
        # تسجيل التدوير
        self.rotation_history.append({
            "timestamp": datetime.now().isoformat(),
            "old_key": old_key[:10] + "...",
            "new_key": new_key[:10] + "...",
            "old_index": old_index,
            "new_index": self.current_index,
            "reason": reason,
            "provider": self.provider,
        })
        
        return new_key, self.current_index
    
    def report_error(self, key: str, error_code: int, error_msg: str = ""):
        """
        تسجيل خطأ لمفتاح معين
        
        Args:
            key: المفتاح الذي حدث الخطأ فيه
            error_code: رمز الخطأ (429, 403, 401, إلخ)
            error_msg: رسالة الخطأ
        """
        self.key_errors[key] += 1
        self.key_last_error[key] = {
            "timestamp": datetime.now().isoformat(),
            "code": error_code,
            "message": error_msg[:100],
        }
        
        # تتبع خاص لأخطاء 429
        if error_code == 429:
            self.key_429_count[key] += 1
            self.key_last_429[key] = datetime.now()
            self.total_429_errors += 1
    
    def report_success(self, key: str):
        """تسجيل طلب ناجح لمفتاح معين"""
        self.key_success_count[key] += 1
    
    def should_rotate_on_429(self) -> bool:
        """التحقق من ما إذا كان يجب التدوير عند خطأ 429"""
        current_key = self.get_current_key()
        
        # إذا كان المفتاح الحالي لديه أخطاء 429 متكررة
        if self.key_429_count[current_key] >= 2:
            return True
        
        # إذا كان لديه خطأ 429 مؤخراً (آخر 5 دقائق)
        last_429 = self.key_last_429[current_key]
        if last_429 and (datetime.now() - last_429).total_seconds() < 300:
            return True
        
        return False
    
    def get_best_key(self) -> str:
        """اختيار أفضل مفتاح بناءً على الأداء"""
        if not self.keys:
            return ""
        
        # حساب درجة الأداء لكل مفتاح
        scores = {}
        for key in self.keys:
            success = self.key_success_count[key]
            errors = self.key_errors[key]
            errors_429 = self.key_429_count[key]
            
            # الصيغة: النجاحات - (الأخطاء * 2) - (أخطاء 429 * 5)
            score = success - (errors * 2) - (errors_429 * 5)
            scores[key] = score
        
        # اختيار المفتاح ذو أعلى درجة
        best_key = max(scores, key=scores.get)
        return best_key
    
    def get_stats(self) -> Dict:
        """الحصول على إحصائيات شاملة"""
        return {
            "provider": self.provider,
            "total_keys": len(self.keys),
            "current_key_index": self.current_index,
            "current_key": self.get_current_key()[:10] + "...",
            "total_rotations": self.total_rotations,
            "total_429_errors": self.total_429_errors,
            "strategy": self.strategy,
            "key_stats": [
                {
                    "key": k[:10] + "...",
                    "successes": self.key_success_count[k],
                    "errors": self.key_errors[k],
                    "errors_429": self.key_429_count[k],
                    "last_error": self.key_last_error[k],
                }
                for k in self.keys
            ],
            "recent_rotations": self.rotation_history[-5:],  # آخر 5 تدويرات
        }
    
    def reset_stats(self):
        """إعادة تعيين الإحصائيات"""
        self.key_errors.clear()
        self.key_last_error.clear()
        self.key_429_count.clear()
        self.key_last_429.clear()
        self.key_success_count.clear()
        self.total_rotations = 0
        self.total_429_errors = 0
        self.rotation_history.clear()


# مديري التدوير العالميون (سيتم تهيئتهم من config.py)
_gemini_rotation_manager: Optional[KeyRotationManager] = None
_openrouter_rotation_manager: Optional[KeyRotationManager] = None
_cohere_rotation_manager: Optional[KeyRotationManager] = None


def init_rotation_managers(gemini_keys: List[str], openrouter_keys: List[str], 
                          cohere_keys: List[str], strategy: str = "round_robin"):
    """تهيئة مديري التدوير"""
    global _gemini_rotation_manager, _openrouter_rotation_manager, _cohere_rotation_manager
    
    if gemini_keys:
        _gemini_rotation_manager = KeyRotationManager(gemini_keys, "gemini", strategy)
    if openrouter_keys:
        _openrouter_rotation_manager = KeyRotationManager(openrouter_keys, "openrouter", strategy)
    if cohere_keys:
        _cohere_rotation_manager = KeyRotationManager(cohere_keys, "cohere", strategy)


def get_gemini_manager() -> Optional[KeyRotationManager]:
    """الحصول على مدير تدوير Gemini"""
    return _gemini_rotation_manager


def get_openrouter_manager() -> Optional[KeyRotationManager]:
    """الحصول على مدير تدوير OpenRouter"""
    return _openrouter_rotation_manager


def get_cohere_manager() -> Optional[KeyRotationManager]:
    """الحصول على مدير تدوير Cohere"""
    return _cohere_rotation_manager


def get_rotation_manager(provider: str) -> Optional[KeyRotationManager]:
    """الحصول على مدير التدوير لمزود معين"""
    if provider.lower() == "gemini":
        return _gemini_rotation_manager
    elif provider.lower() == "openrouter":
        return _openrouter_rotation_manager
    elif provider.lower() == "cohere":
        return _cohere_rotation_manager
    return None
