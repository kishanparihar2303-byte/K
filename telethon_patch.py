"""
telethon_patch.py — Python 3.12 + Telethon 1.36 fix

cryptg install hone ke baad libssl disable karne ki zaroorat nahi.
cryptg fastest AES implementation hai aur GeneratorExit issue bhi nahi karta.

Sirf sys.unraisablehook override rakhte hain — edge cases ke liye.
"""

import sys


def apply():
    """main.py ke top pe call karo."""

    # sys.unraisablehook — belt-and-suspenders for any remaining teardown noise
    _original_unraisablehook = sys.unraisablehook

    def _filtered_unraisablehook(unraisable):
        try:
            exc = unraisable.exc_value
            obj = unraisable.object

            is_generator_exit = isinstance(exc, RuntimeError) and \
                "GeneratorExit" in str(exc)

            is_telethon_loop = False
            try:
                obj_str = str(obj)
                is_telethon_loop = (
                    "telethon" in obj_str.lower() or
                    "_recv_loop" in obj_str or
                    "_send_loop" in obj_str or
                    "Connection" in obj_str
                )
            except Exception:
                pass

            if is_generator_exit and is_telethon_loop:
                return  # Silently drop

        except Exception:
            pass

        _original_unraisablehook(unraisable)

    sys.unraisablehook = _filtered_unraisablehook
    print("[PATCH] Telethon Python 3.12 patch applied")
