import io
import fitz  # PyMuPDF
from PIL import Image, ImageTk
from pyzbar.pyzbar import decode
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from collections import defaultdict
import threading
import queue
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

CONFIG_FILE = "config.json"


def is_qr_code(image_bytes: bytes) -> bool:
    try:
        with Image.open(io.BytesIO(image_bytes)) as im:
            if im.mode not in ("L", "RGB"):
                im = im.convert("RGB")
            return len(decode(im)) > 0
    except Exception:
        return False


def center_window(win, width, height):
    win.update_idletasks()
    screen_width = win.winfo_screenwidth()
    screen_height = win.winfo_screenheight()
    x = int((screen_width / 2) - (width / 2))
    y = int((screen_height / 2) - (height / 2))
    win.geometry(f'{width}x{height}+{x}+{y}')
    win.resizable(False, False)


class PreviewWindow(tk.Toplevel):
    def __init__(self, parent, image_previews, candidates):
        super().__init__(parent)
        self.title("预览并选择要移除的图片")
        self.candidates = candidates
        self.image_vars = {}
        self.thumb_references = []

        center_window(self, 700, 500)

        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(self.main_frame)
        scrollbar = ttk.Scrollbar(self.main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        self._populate_images(scrollable_frame, image_previews)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', pady=5, padx=10)
        ttk.Button(btn_frame, text="确认移除并保存", command=self.confirm_and_save).pack(side='right')
        ttk.Button(btn_frame, text="取消", command=self.destroy).pack(side='right', padx=10)

        self.protocol("WM_DELETE_WINDOW", self.destroy)
        self.transient(parent)
        self.grab_set()
        self.wait_window(self)

    def _populate_images(self, parent_frame, image_previews):
        for category, images in self.candidates.items():
            if not images:
                continue

            cat_frame = ttk.LabelFrame(parent_frame, text=category, padding=(10, 5))
            cat_frame.pack(fill='x', padx=10, pady=5)

            for i, img_data in enumerate(images):
                xref = img_data['xref']
                var = tk.BooleanVar(value=True)
                self.image_vars[xref] = var

                item_frame = ttk.Frame(cat_frame)
                item_frame.pack(fill='x', pady=2)

                chk = ttk.Checkbutton(item_frame, variable=var)
                chk.pack(side='left', padx=5)

                try:
                    img_bytes = image_previews.get(xref)
                    if not img_bytes:
                        raise ValueError("Preview not found")

                    pil_img = Image.open(io.BytesIO(img_bytes))
                    pil_img.thumbnail((80, 80))
                    thumb = ImageTk.PhotoImage(pil_img)
                    self.thumb_references.append(thumb)

                    lbl_thumb = ttk.Label(item_frame, image=thumb)
                    lbl_thumb.pack(side='left')

                    info_text = f"出现在 {len(img_data['pages'])} 个页面上 (例如 P{min(img_data['pages']) + 1})"
                    lbl_info = ttk.Label(item_frame, text=info_text)
                    lbl_info.pack(side='left', padx=10)
                except Exception:
                    lbl_info = ttk.Label(item_frame, text=f"图片 {xref} (无法加载预览)")
                    lbl_info.pack(side='left', padx=10)

    def confirm_and_save(self):
        self.xrefs_to_delete = {xref for xref, var in self.image_vars.items() if var.get()}
        self.destroy()


class MainApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("PDF 图片移除工具")
        center_window(self.root, 480, 320)

        self.file_path = None
        self.analysis_thread = None
        self.progress_queue = queue.Queue()

        self.label = tk.Label(root, text="请选择PDF文件，然后点击分析", anchor="w")
        self.label.pack(fill="x", padx=12, pady=(14, 8))
        btn_frame = tk.Frame(root)
        btn_frame.pack(fill="x", padx=12)
        self.open_btn = tk.Button(btn_frame, text="打开文件", width=15, command=self.open_file)
        self.open_btn.pack(side="left")
        self.analyze_btn = tk.Button(btn_frame, text="分析并预览待移除图片", width=25, command=self.start_analysis,
                                     state="disabled")
        self.analyze_btn.pack(side="left", padx=10)
        options_frame = tk.LabelFrame(root, text="移除规则", padx=10, pady=5)
        options_frame.pack(fill="x", padx=12, pady=10)

        self.remove_qr = tk.BooleanVar()
        self.remove_corners = tk.BooleanVar()
        self.remove_repeated = tk.BooleanVar()
        self.load_config()

        qr_check = tk.Checkbutton(options_frame, text="移除二维码", variable=self.remove_qr, anchor="w")
        qr_check.pack(fill="x")

        corners_check = tk.Checkbutton(options_frame, text="移除边角图片 (Logo/页眉页脚)", variable=self.remove_corners,
                                       anchor="w")
        corners_check.pack(fill="x")

        repeated_check = tk.Checkbutton(options_frame, text="移除重复图片 (水印)", variable=self.remove_repeated, anchor="w")
        repeated_check.pack(fill="x")

        self.status_var = tk.StringVar(value="就绪")
        self.status = tk.Label(root, textvariable=self.status_var, anchor="w", fg="#666")
        self.status.pack(fill="x", padx=12, pady=(5, 0))

        self.progress = ttk.Progressbar(root, orient='horizontal', length=100, mode='determinate')
        self.progress.pack(fill='x', padx=12, pady=(0, 8))

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.remove_qr.set(config.get('remove_qr', True))
                    self.remove_corners.set(config.get('remove_corners', False))
                    self.remove_repeated.set(config.get('remove_repeated', False))
            else:
                self.remove_qr.set(True)
        except (IOError, json.JSONDecodeError):
            self.remove_qr.set(True)

    def save_config(self):
        config = {
            'remove_qr': self.remove_qr.get(),
            'remove_corners': self.remove_corners.get(),
            'remove_repeated': self.remove_repeated.get(),
        }
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config, f, indent=4)
        except IOError:
            pass

    def on_closing(self):
        self.save_config()
        self.root.destroy()

    def open_file(self):
        file = filedialog.askopenfilename(title="选择PDF文件", filetypes=[("PDF Files", "*.pdf"), ("All Files", "*.*")])
        if not file:
            return
        self.file_path = file
        self.label.config(text=f"已打开: {os.path.basename(file)}")
        try:
            with fitz.open(file) as doc:
                self.status_var.set(f"共 {len(doc)} 页。请选择规则并点击分析。")
            self.analyze_btn.config(state="normal")
        except Exception as e:
            messagebox.showerror("打开失败", f"无法打开 PDF\n{e}")
            self.analyze_btn.config(state="disabled")

    def start_analysis(self):
        if not self.file_path:
            return

        rules = {
            'qr': self.remove_qr.get(),
            'corners': self.remove_corners.get(),
            'repeated': self.remove_repeated.get()
        }

        if not any(rules.values()):
            messagebox.showinfo("未选择规则", "请至少勾选一个移除规则。")
            return

        self.analyze_btn.config(state="disabled")
        self.open_btn.config(state="disabled")
        self.progress['value'] = 0
        self.status_var.set("开始分析...")

        self.analysis_thread = threading.Thread(target=self._worker_analyze, args=(self.file_path, rules), daemon=True)
        self.analysis_thread.start()
        self.root.after(100, self.check_progress)

    def check_progress(self):
        try:
            msg = self.progress_queue.get_nowait()
            if isinstance(msg, dict):
                if 'progress' in msg:
                    self.progress['value'] = msg['progress']
                    status_text = msg.get('status', '正在分析...')
                    self.status_var.set(f"{status_text} {int(msg['progress'])}%")
                elif 'result' in msg:
                    self.analysis_thread = None
                    self.analyze_btn.config(state="normal")
                    self.open_btn.config(state="normal")
                    self.progress['value'] = 100
                    self.status_var.set("分析完成，请在预览窗口中确认。")
                    self._handle_analysis_result(msg['result'])
                elif 'error' in msg:
                    raise Exception(msg['error'])
            self.root.after(100, self.check_progress)
        except queue.Empty:
            self.root.after(100, self.check_progress)
        except Exception as e:
            self.analysis_thread = None
            self.analyze_btn.config(state="normal")
            self.open_btn.config(state="normal")
            messagebox.showerror("分析失败", str(e))
            self.status_var.set("分析失败")

    def _worker_analyze(self, file_path, rules):
        try:
            doc = fitz.open(file_path)
            total_pages = len(doc)
            image_map = defaultdict(lambda: {'pages': set(), 'placements': []})

            # build an image map
            for page_num in range(total_pages):
                page = doc.load_page(page_num)
                for img in page.get_image_info(xrefs=True):
                    xref = img['xref']
                    if xref == 0: continue
                    image_map[xref]['pages'].add(page_num)
                    image_map[xref]['placements'].append(
                        {'page_num': page_num, 'bbox': fitz.Rect(img['bbox']), 'page_rect': page.rect})
                progress = (page_num + 1) / total_pages * 20
                self.progress_queue.put({'progress': progress, 'status': '扫描页面...'})

            # extract image bytes and prepare tasks
            image_tasks = []
            for xref, info in image_map.items():
                try:
                    img_obj = doc.extract_image(xref)
                    img_bytes = img_obj.get("image")
                    if img_bytes:
                        image_tasks.append({
                            'xref': xref,
                            'bytes': img_bytes,
                            'info': info
                        })
                except Exception:
                    continue

            doc.close()

            # analyze images
            candidates = defaultdict(list)
            total_images = len(image_tasks)
            processed_count = 0

            with ThreadPoolExecutor() as executor:
                future_to_task = {executor.submit(self._analyze_single_image, task, rules, total_pages): task for task
                                  in image_tasks}
                for future in as_completed(future_to_task):
                    try:
                        result = future.result()
                        if result:
                            category, xref, pages = result
                            candidates[category].append({'xref': xref, 'pages': pages})
                    except Exception:
                        pass
                    finally:
                        processed_count += 1
                        progress = 20 + (processed_count / total_images * 80)
                        self.progress_queue.put({'progress': progress, 'status': '分析图片...'})

            image_previews = {task['xref']: task['bytes'] for task in image_tasks}
            self.progress_queue.put({'result': (candidates, image_previews)})

        except Exception as e:
            self.progress_queue.put({'error': str(e)})

    def _analyze_single_image(self, task, rules, total_pages):
        xref, img_bytes, info = task['xref'], task['bytes'], task['info']
        pages = info['pages']

        if rules['qr'] and is_qr_code(img_bytes):
            return '二维码', xref, pages

        if rules['repeated'] and total_pages > 1 and len(pages) >= total_pages * 0.7:
            return '重复图片 (水印)', xref, pages

        if rules['corners']:
            for placement in info['placements']:
                page_rect = placement['page_rect']
                img_rect = placement['bbox']
                margin_x = page_rect.width * 0.25
                margin_y = page_rect.height * 0.15
                if (img_rect.x1 < margin_x and img_rect.y1 < margin_y) or \
                        (img_rect.x0 > page_rect.width - margin_x and img_rect.y1 < margin_y):
                    return '边角图片', xref, pages
        return None

    def _handle_analysis_result(self, result):
        candidates, image_previews = result
        if not any(candidates.values()):
            messagebox.showinfo("未发现图片", "未找到符合移除规则的图片。")
            self.status_var.set("就绪")
            return

        preview = PreviewWindow(self.root, image_previews, candidates)
        if hasattr(preview, 'xrefs_to_delete') and preview.xrefs_to_delete:
            self._save_with_deletions(preview.xrefs_to_delete)

        self.analyze_btn.config(state="normal")
        self.status_var.set("就绪")
        self.label.config(text="请选择PDF文件，然后点击分析")

    def _save_with_deletions(self, xrefs_to_delete):
        save_path = filedialog.asksaveasfilename(title="保存PDF文件", defaultextension=".pdf",
                                                 filetypes=[("PDF Files", "*.pdf"), ("All Files", "*.*")])
        if not save_path:
            return

        try:
            temp_doc = fitz.open(self.file_path)
            removed_count = 0
            affected_pages = set()

            for page_num in range(len(temp_doc)):
                page = temp_doc.load_page(page_num)
                page_removed = False
                for img_info in page.get_image_info(xrefs=True):
                    if img_info['xref'] in xrefs_to_delete:
                        page.delete_image(img_info['xref'])
                        removed_count += 1
                        page_removed = True
                if page_removed:
                    affected_pages.add(page_num + 1)

            temp_doc.save(save_path, garbage=4, deflate=True, clean=True)
            temp_doc.close()
            summary = f"完成：共移除 {removed_count} 处图片，涉及 {len(affected_pages)} 页。"
            messagebox.showinfo("保存成功", summary)
            self.status_var.set(summary)
        except Exception as e:
            messagebox.showerror("保存失败", f"保存文件时出错:\n{e}")


def main():
    root = tk.Tk()
    app = MainApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
