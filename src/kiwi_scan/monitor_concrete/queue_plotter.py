# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import threading
import time
import queue
import logging
# TODO: lazy imports for headless applications?
import matplotlib
matplotlib.use('TkAgg')  # BEFORE importing pyplot TODO: this was to overcome some host limitations, do not hard code here
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import ttk
from kiwi_scan.monitor.base import BaseMonitor

class QueuePlotterMonitor(BaseMonitor):
    def __init__(self):
        self.queue = queue.Queue()
        self.root = None  # Will be set in start()

    def start(self, signal_names):
        # Create root window FIRST, then StringVar
        self._start_time = time.time()
        self.root = tk.Tk()
        self.signal_names = ["t"] + signal_names
        logging.debug(f"signal_names: {self.signal_names}")
        self.data = {name: [] for name in self.signal_names}
        self.x_signal = tk.StringVar(self.root, value=self.signal_names[0])
        self.y_signal = tk.StringVar(self.root, value=self.signal_names[1])
        self.running = True
        self._start_gui()  # create widgets/vars

    def update(self, vals):
        values_only = [v['value'] for v in vals]
        now = time.time() - self._start_time  # or just time.time() for absolute time
        # Make sure the point dict always has all keys in self.signal_names
        values_dict = dict(zip(self.signal_names, [now] + values_only))
        # The dict comprehension ensures all expected keys, even if missing values
        point = {k: values_dict.get(k, float("nan")) for k in self.signal_names}
        self.queue.put(point)

    def set_signals(self, x, y):
        self.x_signal.set(x)
        self.y_signal.set(y)

    def _start_gui(self):
        def on_select(event=None):
            x = self.x_signal.get()
            y = self.y_signal.get()
            if x != y:
                logging.debug(f"Changed axes to X: {x}, Y: {y}")

        self.root.title("Select Plot Axes")
        ttk.Label(self.root, text="X Axis:").grid(row=0, column=0)
        ttk.Label(self.root, text="Y Axis:").grid(row=1, column=0)

        x_menu = ttk.Combobox(self.root, textvariable=self.x_signal, values=self.signal_names, state="readonly")
        y_menu = ttk.Combobox(self.root, textvariable=self.y_signal, values=self.signal_names, state="readonly")
        x_menu.grid(row=0, column=1)
        y_menu.grid(row=1, column=1)
        x_menu.bind('<<ComboboxSelected>>', on_select)
        y_menu.bind('<<ComboboxSelected>>', on_select)

        # Start GUI event loop in a separate thread
        threading.Thread(target=self.root.mainloop, daemon=True).start()

    def loop(self):
        logging.info(f"[{threading.current_thread().name}] In monitor.loop(), about to start Tkinter mainloop")
        plt.ion()
        fig, ax = plt.subplots()
        while self.running or not self.queue.empty():
            try:
                while True:
                    point = self.queue.get_nowait()
                    logging.debug(f"point: {point}")
                    for k in self.signal_names:
                        self.data[k].append(point[k])
            except queue.Empty:
                pass
            x = self.x_signal.get()
            y = self.y_signal.get()
            if x != y and len(self.data[x]) > 0 and len(self.data[y]) > 0:
                ax.clear()
                # logging.debug(f"x:{self.data[x]}, y:{self.data[y]}")
                ax.plot(self.data[x], self.data[y], marker='o')
                ax.set_xlabel(x)
                ax.set_ylabel(y)
                ax.set_title("Live Scan Data")
            plt.pause(0.05)
        plt.ioff()
        plt.show()

    def close(self):
        self.running = False
        if self.root:
            try:
                self.root.quit()
            except Exception:
                pass

