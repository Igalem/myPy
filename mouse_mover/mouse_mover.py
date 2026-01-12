import tkinter as tk
from tkinter import messagebox
import pyautogui
import threading
import time
import random

# Configure PyAutoGUI
pyautogui.FAILSAFE = True  # Move mouse to top-left to stop
pyautogui.PAUSE = 0.1  # Small pause between actions

class MouseMoverApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Mouse Mover")
        self.root.geometry("300x200")
        self.is_running = False
        self.thread = None

        # GUI Elements
        self.label = tk.Label(root, text="Interval (seconds):")
        self.label.pack(pady=10)

        self.interval_entry = tk.Entry(root)
        self.interval_entry.insert(0, "5.0")
        self.interval_entry.pack(pady=5)

        self.start_button = tk.Button(root, text="Start", command=self.start_moving)
        self.start_button.pack(pady=5)

        self.stop_button = tk.Button(root, text="Stop", command=self.stop_moving, state=tk.DISABLED)
        self.stop_button.pack(pady=5)

        self.status_label = tk.Label(root, text="Status: Stopped")
        self.status_label.pack(pady=10)

    def move_mouse(self):
        while self.is_running:
            try:
                interval = float(self.interval_entry.get())
                if interval <= 0:
                    raise ValueError("Interval must be positive")
            except ValueError:
                messagebox.showerror("Error", "Please enter a valid positive number for interval")
                self.stop_moving()
                return

            # Get current mouse position
            x, y = pyautogui.position()

            # Move in a small square with slight random offset
            for _ in range(4):
                if not self.is_running:
                    break
                offset = random.randint(-10, 10)
                pyautogui.moveTo(x + 50 + offset, y, duration=0.2)
                pyautogui.moveTo(x + 50 + offset, y + 50 + offset, duration=0.2)
                pyautogui.moveTo(x, y + 50 + offset, duration=0.2)
                pyautogui.moveTo(x, y, duration=0.2)
                time.sleep(interval)

    def start_moving(self):
        if not self.is_running:
            self.is_running = True
            self.start_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
            self.status_label.config(text="Status: Running")
            self.thread = threading.Thread(target=self.move_mouse)
            self.thread.daemon = True  # Thread exits when app closes
            self.thread.start()

    def stop_moving(self):
        if self.is_running:
            self.is_running = False
            self.start_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            self.status_label.config(text="Status: Stopped")
            if self.thread:
                self.thread.join()

if __name__ == "__main__":
    root = tk.Tk()
    app = MouseMoverApp(root)
    root.mainloop()