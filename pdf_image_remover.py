import io
import fitz  # PyMuPDF
from PIL import Image, ImageTk
from pyzbar.pyzbar import decode
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from collections import defaultdict
import multiprocessing
from queue import Empty
import json
import os
import math
import hashlib
from typing import Optional

from tkinterdnd2 import DND_FILES, TkinterDnD

CONFIG_FILE = "config.json"
VERSION = "1.1"
ICON_PATH = "pdf_image_remover_icon.ico"


def center_window(win, width, height):
    win.update_idletasks()
    screen_width = win.winfo_screenwidth()
    screen_height = win.winfo_screenheight()
    x = int((screen_width / 2) - (width / 2))
    y = int((screen_height / 2) - (height / 2))
    win.geometry(f'{width}x{height}+{x}+{y}')
    win.resizable(False, False)


def is_qr_code(image_bytes: bytes) -> bool:
    try:
        with Image.open(io.BytesIO(image_bytes)) as im:
            width, height = im.size
            if width < 30 or height < 30:
                return False
            if height == 0:
                return False
            aspect_ratio = width / height
            if not (0.9 < aspect_ratio < 1.1):
                return False
            if im.mode not in ("L", "RGB"):
                im = im.convert("RGB")
            return len(decode(im)) > 0
    except Exception:
        return False


def _analyze_single_image_ret(task, rules, total_pages):
    try:
        xref, img_bytes, info = task['xref'], task['bytes'], task['info']
        pages = info['pages']

        if rules.get('qr') and is_qr_code(img_bytes):
            return '二维码', xref, pages

        if rules.get('repeated') and total_pages > 1 and len(pages) >= total_pages * 0.7:
            return '重复图片 (水印)', xref, pages

        if rules.get('corners'):
            for placement in info['placements']:
                x0, y0, x1, y1 = placement['bbox']
                page_w, page_h = placement['page_size']
                margin_x = page_w * 0.25
                margin_y = page_h * 0.15
                if (x1 < margin_x and y1 < margin_y) or \
                        (x0 > page_w - margin_x and y1 < margin_y):
                    return '边角图片', xref, pages
        return None
    except Exception:
        return None


def _analyze_single_image(task, rules, total_pages, q):
    res = _analyze_single_image_ret(task, rules, total_pages)
    if res is not None:
        q.put(('image_result', res))
    else:
        q.put(('image_result', None))


def _process_page_chunk_ret_star(args):
    return _process_page_chunk_ret(*args)


def _process_page_chunk_ret(file_path, start_page, end_page):
    try:
        image_map_chunk = defaultdict(lambda: {'pages': set(), 'placements': []})
        image_bytes_chunk = {}
        hash_to_xref = {}
        doc = fitz.open(file_path)
        for page_num in range(start_page, end_page):
            if page_num >= len(doc):
                continue
            page = doc.load_page(page_num)
            for img in page.get_image_info(xrefs=True):
                xref = img['xref']
                if xref == 0:
                    continue
                try:
                    img_obj = doc.extract_image(xref)
                    img_bytes = img_obj.get("image")
                    if not img_bytes:
                        continue
                    with Image.open(io.BytesIO(img_bytes)) as im:
                        width, height = im.size
                        if min(width, height) <= 8 or (width <= 6 and height <= 6) or (width * height) <= 64:   # 小图过滤
                            continue
                except Exception:
                    continue
                image_map_chunk[xref]['pages'].add(page_num)
                rect = fitz.Rect(img['bbox'])
                bbox_tuple = (rect.x0, rect.y0, rect.x1, rect.y1)
                page_size = (page.rect.width, page.rect.height)
                image_map_chunk[xref]['placements'].append(
                    {'page_num': page_num, 'bbox': bbox_tuple, 'page_size': page_size})
                if xref not in image_bytes_chunk:
                    image_bytes_chunk[xref] = img_bytes
        doc.close()
        serializable_map = {}
        for xref, data in image_map_chunk.items():
            serializable_map[int(xref)] = {
                'pages': list(data['pages']),
                'placements': data['placements'],
            }
        return serializable_map, image_bytes_chunk
    except Exception as e:
        return {'__error__': str(e)}, {}


def _process_page_chunk(file_path, start_page, end_page, q):
    try:
        image_map_chunk = defaultdict(lambda: {'pages': set(), 'placements': []})
        image_bytes_chunk = {}
        doc = fitz.open(file_path)
        for page_num in range(start_page, end_page):
            if page_num >= len(doc):
                continue
            page = doc.load_page(page_num)
            for img in page.get_image_info(xrefs=True):
                xref = img['xref']
                if xref == 0:
                    continue
                try:
                    img_obj = doc.extract_image(xref)
                    img_bytes = img_obj.get("image")
                    if not img_bytes:
                        continue
                    with Image.open(io.BytesIO(img_bytes)) as im:
                        width, height = im.size
                        if min(width, height) <= 8 or (width <= 6 and height <= 6) or (width * height) <= 64:
                            continue
                except Exception:
                    continue

                image_map_chunk[xref]['pages'].add(page_num)
                rect = fitz.Rect(img['bbox'])
                bbox_tuple = (rect.x0, rect.y0, rect.x1, rect.y1)
                page_size = (page.rect.width, page.rect.height)
                image_map_chunk[xref]['placements'].append(
                    {'page_num': page_num, 'bbox': bbox_tuple, 'page_size': page_size})
                if xref not in image_bytes_chunk:
                    image_bytes_chunk[xref] = img_bytes
        doc.close()

        serializable_map = {}
        for xref, data in image_map_chunk.items():
            serializable_map[int(xref)] = {
                'pages': list(data['pages']),
                'placements': data['placements'],
            }
        q.put(('chunk_result', (serializable_map, image_bytes_chunk)))
    except Exception as e:
        q.put(('error', f"页面扫描失败: {e}"))

    def save_with_deletions(self, save_path: str, xrefs_to_delete: set):
        temp_doc = fitz.open(self.file_path)
        removed_count = 0
        affected_pages = set()
        try:
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
        finally:
            temp_doc.close()
        return removed_count, affected_pages


class PdfImageProcessor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def start_phase1(self, ui_queue):
        try:
            doc = fitz.open(self.file_path)
            total_pages = len(doc)
            doc.close()
        except Exception as e:
            ui_queue.put(('error', str(e)))
            return
        num_workers = max(1, min(os.cpu_count() or 1, 4))
        chunk_size = max(1, math.ceil(total_pages / num_workers))
        page_chunks = [(i, min(i + chunk_size, total_pages)) for i in range(0, total_pages, chunk_size)]
        ui_queue.put(('phase1_started', (len(page_chunks), total_pages)))
        ui_queue.put(('progress', {'progress': 0, 'status': '1/2 准备扫描页面...'}))

        try:
            with multiprocessing.Pool(processes=num_workers) as pool:
                for map_chunk, bytes_chunk in pool.imap_unordered(
                        _process_page_chunk_ret_star,
                        [(self.file_path, s, e) for s, e in page_chunks],
                        chunksize=1,
                ):

                    if isinstance(map_chunk, dict) and map_chunk.get('__error__'):
                        ui_queue.put(('error', f"页面扫描失败: {map_chunk['__error__']}"))
                        return
                    ui_queue.put(('page_chunk_result', (map_chunk, bytes_chunk)))
        except Exception as e:
            ui_queue.put(('error', f'页面扫描进程异常: {e}'))
            return

    def start_phase2(self, image_tasks, rules, total_pages, ui_queue):
        result_q = multiprocessing.Queue()
        procs = [multiprocessing.Process(target=_analyze_single_image, args=(task, rules, total_pages, result_q),
                                         daemon=True) for task in image_tasks]
        for p in procs:
            p.start()
        candidates = defaultdict(list)
        image_previews = {task['xref']: task['bytes'] for task in image_tasks}
        total = len(image_tasks)
        completed = 0
        while completed < total:
            try:
                msg_type, payload = result_q.get()
                if msg_type == 'image_result':
                    completed += 1
                    result = payload
                    if result:
                        category, xref, pages = result
                        candidates[category].append({'xref': xref, 'pages': pages})
                    ui_queue.put(('progress', {
                        'progress': 50 + (completed / total) * 50,
                        'status': f'2/2 分析图片... ({completed}/{total})'
                    }))
                elif msg_type == 'error':
                    completed += 1
                    ui_queue.put(('error', payload))
            except Exception as e:
                ui_queue.put(('error', f'图片分析进程异常: {e}'))
                break
        for p in procs:
            try:
                p.join()
            except Exception:
                pass
        ui_queue.put(('image_analysis_result', (candidates, image_previews)))

    def save_with_deletions(self, save_path: str, xrefs_to_delete: set):
        temp_doc = fitz.open(self.file_path)
        removed_count = 0
        affected_pages = set()
        try:
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
        finally:
            temp_doc.close()
        return removed_count, affected_pages


class PreviewWindow(tk.Toplevel):
    def __init__(self, parent, image_previews, candidates):
        super().__init__(parent)
        self.title("预览并选择要移除的图片")
        try:
            if os.path.exists(ICON_PATH):
                self.iconbitmap(ICON_PATH)
        except Exception:
            pass

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

            for _, img_data in enumerate(images):
                xref = img_data['xref']
                var = tk.BooleanVar(value=True)
                self.image_vars[xref] = var

                item_frame = ttk.Frame(cat_frame)
                item_frame.pack(fill='x', pady=2)

                ttk.Checkbutton(item_frame, variable=var).pack(side='left', padx=5)

                try:
                    img_bytes = image_previews.get(xref)
                    if not img_bytes:
                        raise ValueError("Preview not found")

                    pil_img = Image.open(io.BytesIO(img_bytes))
                    pil_img.thumbnail((80, 80))
                    thumb = ImageTk.PhotoImage(pil_img)
                    self.thumb_references.append(thumb)

                    ttk.Label(item_frame, image=thumb).pack(side='left')

                    info_text = f"出现在 {len(img_data['pages'])} 个页面上 (例如 P{min(img_data['pages']) + 1})"
                    ttk.Label(item_frame, text=info_text).pack(side='left', padx=10)
                except Exception:
                    ttk.Label(item_frame, text=f"图片 {xref} (无法加载预览)").pack(side='left', padx=10)

    def confirm_and_save(self):
        self.xrefs_to_delete = {xref for xref, var in self.image_vars.items() if var.get()}
        self.destroy()


class AnalysisRunner:
    def __init__(self, root: tk.Tk, processor: PdfImageProcessor, on_progress, on_result, on_error):
        self.root = root
        self.processor = processor
        self.on_progress = on_progress
        self.on_result = on_result
        self.on_error = on_error

        self.q = multiprocessing.Queue()
        self.analysis_process = None
        self._polling = False

        self.phase = None
        self.rules = {}
        self.total_pages = 0
        self.total_chunks = 0
        self.completed_chunks = 0
        self.total_images = 0
        self.completed_images = 0

        self.image_map = defaultdict(lambda: {'pages': set(), 'placements': []})
        self.image_bytes_map = {}

    def start(self, rules: dict):
        self._reset_state()
        self.rules = rules

        import threading
        threading.Thread(target=self.processor.start_phase1, args=(self.q,), daemon=True).start()

        if not self._polling:
            self._polling = True
            self.root.after(50, self._schedule_poll)

    def _reset_state(self):
        self.phase = 'phase1_running'
        self.image_map.clear()
        self.image_bytes_map.clear()
        self.completed_chunks = 0
        self.completed_images = 0

    def _schedule_poll(self):
        try:
            for _ in range(100):
                msg_type, data = self.q.get_nowait()

                if msg_type == 'error':
                    self.on_error(data)
                    self._polling = False
                    return

                if self.phase == 'phase1_running':
                    if msg_type == 'phase1_started':
                        self.total_chunks, self.total_pages = data
                    elif msg_type == 'page_chunk_result':
                        map_chunk, bytes_chunk = data
                        self._aggregate_chunk_data(map_chunk, bytes_chunk)
                        self.completed_chunks += 1
                        self.on_progress({
                            'progress': (self.completed_chunks / self.total_chunks) * 50,
                            'status': f'1/2 扫描页面... ({self.completed_chunks}/{self.total_chunks})'
                        })
                        if self.completed_chunks == self.total_chunks:
                            self._start_phase2()

                elif self.phase == 'phase2_running':
                    if msg_type == 'progress':
                        self.on_progress(data)
                    elif msg_type == 'image_analysis_result':
                        candidates, image_previews = data
                        self.on_result((candidates, image_previews))
                        self._polling = False
                        return

        except Empty:
            pass
        finally:
            if self._polling:
                self.root.after(50, self._schedule_poll)

    def _aggregate_chunk_data(self, map_chunk, bytes_chunk):
        for xref, data in map_chunk.items():
            self.image_map[xref]['pages'].update(data['pages'])
            self.image_map[xref]['placements'].extend(data['placements'])
        self.image_bytes_map.update(bytes_chunk)

    def _start_phase2(self):
        self.phase = 'phase2_running'
        image_tasks = [{'xref': xref, 'bytes': img_bytes, 'info': info} for xref, info in self.image_map.items() if
                       (img_bytes := self.image_bytes_map.get(xref))]
        self.total_images = len(image_tasks)

        if not image_tasks:
            self.on_result((defaultdict(list), {}))
            self._polling = False
            return

        import threading
        threading.Thread(target=self.processor.start_phase2, args=(image_tasks, self.rules, self.total_pages, self.q),
                         daemon=True).start()


class FileSaver:
    def __init__(self, filetypes=None):
        self.filetypes = filetypes or [("PDF Files", "*.pdf"), ("All Files", "*.*")]

    def save_with_dialog(self, processor: PdfImageProcessor, xrefs_to_delete: set):
        in_dir = os.path.dirname(processor.file_path)
        base = os.path.splitext(os.path.basename(processor.file_path))[0]
        default_name = f"{base}_cleaned.pdf"
        save_path = filedialog.asksaveasfilename(
            title="保存PDF文件",
            defaultextension=".pdf",
            initialdir=in_dir if os.path.isdir(in_dir) else None,
            initialfile=default_name,
            filetypes=self.filetypes,
        )
        if not save_path:
            return None
        try:
            removed_count, affected_pages = processor.save_with_deletions(save_path, xrefs_to_delete)
            summary = f"完成：共移除 {removed_count} 处图片，涉及 {len(affected_pages)} 页。"
            messagebox.showinfo("保存成功", summary)
            return summary
        except Exception as e:
            messagebox.showerror("保存失败", f"保存文件时出错:\n{e}")
            return None

    def choose_output_dir(self):
        return filedialog.askdirectory(title="选择输出文件夹")

    def auto_save(self, processor: PdfImageProcessor, xrefs_to_delete: set, input_path: str, output_dir: str):
        base = os.path.splitext(os.path.basename(input_path))[0]
        out_path = os.path.join(output_dir, f"{base}_cleaned.pdf")
        removed_count, affected_pages = processor.save_with_deletions(out_path, xrefs_to_delete)
        return out_path, removed_count, affected_pages


class ProgressController:
    def __init__(self, root: tk.Tk, bar_widget: ttk.Progressbar, status_var: tk.StringVar):
        self.root = root
        self.bar = bar_widget
        self.status_var = status_var
        self._anim_job = None
        self._target = 0.0
        self._status = ''
        self._run_id = 0

    def start(self, status: str = '就绪') -> int:
        self._run_id += 1
        self._target = 0.0
        try:
            self.bar['value'] = 0
        except Exception:
            pass
        self._status = status
        self._kick()
        return self._run_id

    def update(self, target: float, status: Optional[str] = None, run_id: Optional[int] = None):
        if run_id is not None and run_id != self._run_id:
            return
        try:
            target = float(target)
        except Exception:
            return
        target = max(0.0, min(100.0, target))
        self._target = target
        if status is not None:
            self._status = status
        if self._anim_job is None:
            self._kick()

    def complete(self, run_id: Optional[int] = None):
        self.update(100.0, run_id=run_id)

    def cancel(self, run_id: Optional[int] = None):
        if run_id is not None and run_id != self._run_id:
            return
        if self._anim_job:
            try:
                self.root.after_cancel(self._anim_job)
            except Exception:
                pass
        self._anim_job = None

    def _kick(self):
        self._anim_job = self.root.after(33, self._step)

    def _step(self):
        try:
            cur = float(self.bar['value'])
        except Exception:
            cur = 0.0
        tgt = self._target
        delta = tgt - cur
        if abs(delta) < 0.5:
            try:
                self.bar['value'] = tgt
            except Exception:
                pass
            self.status_var.set(f"{self._status} {int(tgt)}%")
            self._anim_job = None
            return
        step = max(abs(delta) * 0.2, 0.5)
        cur = cur + step if delta > 0 else cur - step
        try:
            self.bar['value'] = cur
        except Exception:
            pass
        self.status_var.set(f"{self._status} {int(cur)}%")
        self._anim_job = self.root.after(33, self._step)


class MainApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(f"PDF 图片移除工具 v{VERSION}")
        try:
            if os.path.exists(ICON_PATH):
                self.root.iconbitmap(ICON_PATH)
        except Exception:
            pass
        center_window(self.root, 720, 420)

        self.remove_qr = tk.BooleanVar()
        self.remove_corners = tk.BooleanVar()
        self.remove_repeated = tk.BooleanVar()
        self.load_config()

        left_frame = ttk.Frame(root)
        left_frame.pack(side='left', fill='both', expand=True, padx=10, pady=10)

        ttk.Label(left_frame, text="文件列表（可拖拽PDF到此处）").pack(anchor='w')
        self.file_list = tk.Listbox(left_frame, selectmode=tk.EXTENDED, height=12)
        self.file_list.pack(fill='both', expand=True)

        try:
            self.file_list.drop_target_register(DND_FILES)
            self.file_list.dnd_bind('<<Drop>>', self._on_drop)
        except Exception:
            pass

        list_btn_frame = ttk.Frame(left_frame)
        list_btn_frame.pack(fill='x', pady=(6, 0))
        ttk.Button(list_btn_frame, text="添加文件", width=12, command=self.add_files).pack(side='left')
        ttk.Button(list_btn_frame, text="移除选中", width=12, command=self.remove_selected).pack(side='left', padx=6)
        ttk.Button(list_btn_frame, text="清空列表", width=12, command=self.clear_list).pack(side='left')

        right_frame = ttk.Frame(root)
        right_frame.pack(side='right', fill='y', padx=10, pady=10)

        options_frame = ttk.LabelFrame(right_frame, text="移除规则", padding=(10, 8))
        options_frame.pack(fill='x')
        ttk.Checkbutton(options_frame, text="移除二维码", variable=self.remove_qr).pack(anchor='w')
        ttk.Checkbutton(options_frame, text="移除边角图片 (Logo/页眉页脚)", variable=self.remove_corners).pack(anchor='w')
        ttk.Checkbutton(options_frame, text="移除重复图片 (水印)", variable=self.remove_repeated).pack(anchor='w')

        action_frame = ttk.LabelFrame(right_frame, text="操作", padding=(10, 8))
        action_frame.pack(fill='x', pady=(10, 0))
        self.preview_btn = ttk.Button(action_frame, text="分析并预览选中项", command=self.start_interactive, state='disabled')
        self.preview_btn.pack(fill='x')
        self.auto_btn = ttk.Button(action_frame, text="全自动处理所有文件", command=self.start_auto, state='disabled')
        self.auto_btn.pack(fill='x', pady=(6, 0))

        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(right_frame, textvariable=self.status_var, foreground="#666").pack(fill='x', pady=(10, 0))
        self.progress = ttk.Progressbar(right_frame, orient='horizontal', mode='determinate', length=200)
        self.progress.pack(fill='x', pady=(6, 0))

        self.progress_ctl = ProgressController(self.root, self.progress, self.status_var)

        self.processor = None
        self.runner = None
        self.file_saver = FileSaver()
        self.batch_queue = []
        self.batch_mode = None
        self.output_dir = None

        self.file_list.bind('<<ListboxSelect>>', self._on_list_select)
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

    def _rules(self):
        return {
            'qr': self.remove_qr.get(),
            'corners': self.remove_corners.get(),
            'repeated': self.remove_repeated.get()
        }

    def _on_list_select(self, _evt=None):
        has_items = self.file_list.size() > 0

        if getattr(self, 'is_running', False):
            self.preview_btn.config(state='disabled')
            self.auto_btn.config(state='disabled')
            return
        self.preview_btn.config(state='normal' if has_items else 'disabled')
        self.auto_btn.config(state='normal' if has_items else 'disabled')

    def add_files(self):
        files = filedialog.askopenfilenames(title="选择PDF文件", filetypes=[("PDF Files", "*.pdf"), ("All Files", "*.*")])
        if not files:
            return
        for f in files:
            if f.lower().endswith('.pdf') and f not in self._all_files():
                self.file_list.insert('end', f)
        self._on_list_select()

    def remove_selected(self):
        selected = list(self.file_list.curselection())
        for idx in reversed(selected):
            self.file_list.delete(idx)
        self._on_list_select()

    def clear_list(self):
        self.file_list.delete(0, 'end')
        self._on_list_select()

    def _all_files(self):
        return [self.file_list.get(i) for i in range(self.file_list.size())]

    def _selected_files(self):
        sel = self.file_list.curselection()
        if not sel:
            return []
        return [self.file_list.get(i) for i in sel]

    def _on_drop(self, event):
        raw = event.data
        paths = []
        buf = ''
        in_quote = False
        for ch in raw:
            if ch == '"':
                in_quote = not in_quote
            elif ch == ' ' and not in_quote:
                if buf:
                    paths.append(buf)
                    buf = ''
            else:
                buf += ch
        if buf:
            paths.append(buf)
        for p in paths:
            p = p.strip().strip('{').strip('}')
            if p.lower().endswith('.pdf') and os.path.exists(p) and p not in self._all_files():
                self.file_list.insert('end', p)
        self._on_list_select()

    def start_interactive(self):
        files = self._selected_files() or self._all_files()
        if not files:
            messagebox.showinfo("无文件", "请先添加文件到列表。")
            return
        self.batch_mode = 'interactive'
        self.batch_queue = list(files)
        self._set_running(True, f"开始交互式处理（{len(files)} 个文件）...")
        # 启动新的进度run id
        self.current_run_id = self.progress_ctl.start('准备分析...')
        self._interactive_next()

    def _interactive_next(self):
        if not self.batch_queue:
            self._set_running(False, "就绪")
            return
        current = self.batch_queue.pop(0)
        self.status_var.set(f"分析：{os.path.basename(current)}")
        self.progress['value'] = 0
        self.processor = PdfImageProcessor(current)
        self.runner = AnalysisRunner(self.root, self.processor, self._on_progress, self._on_result_interactive,
                                     self._on_error)
        self.runner.start(self._rules())

    def _on_result_interactive(self, result):
        candidates, image_previews = result

        has_candidates = any(candidates.values())

        if not has_candidates:
            is_single_file = len(self.batch_queue) == 0 and self.processor is not None

            if is_single_file:
                messagebox.showinfo(
                    "未发现图片",
                    f"未找到符合移除规则的图片。\n\n"
                    f"文件：{os.path.basename(self.processor.file_path)}\n\n"
                    f"请检查：\n"
                    f"• 是否选择了正确的移除规则\n"
                    f"• PDF中是否包含符合规则的图片"
                )
                self.status_var.set("未发现符合规则的图片")
            else:
                self.status_var.set(f"跳过：{os.path.basename(self.processor.file_path)}（无符合规则的图片）")

            self._interactive_next()
            return

        preview = PreviewWindow(self.root, image_previews, candidates)
        if hasattr(preview, 'xrefs_to_delete') and preview.xrefs_to_delete:
            summary = self.file_saver.save_with_dialog(self.processor, preview.xrefs_to_delete)
            if summary:
                self.status_var.set(summary)
        self._interactive_next()

    def start_auto(self):
        files = self._all_files()
        if not files:
            messagebox.showinfo("无文件", "请先添加文件到列表。")
            return
        out_dir = self.file_saver.choose_output_dir()
        if not out_dir:
            return
        if not os.path.isdir(out_dir):
            messagebox.showerror("目录无效", "请选择有效的输出文件夹。")
            return
        self.output_dir = out_dir
        self.batch_mode = 'auto'
        self.batch_queue = list(files)
        # 初始化批处理报告
        self.auto_report = []
        self._set_running(True, f"开始全自动处理（输出到：{out_dir}）...")
        # 启动新的进度run id
        self.current_run_id = self.progress_ctl.start('准备分析...')
        self._auto_next()

    def _auto_next(self):
        if not self.batch_queue:
            self._set_running(False, "全自动处理完成")
            if getattr(self, 'auto_report', None):
                lines = []
                success_cnt = 0
                skip_cnt = 0
                for item in self.auto_report:
                    if item['removed'] > 0:
                        success_cnt += 1
                        lines.append(
                            f"✓ {os.path.basename(item['file'])}：移除 {item['removed']} 处，{len(item['pages'])} 页 → {os.path.basename(item['out'])}")
                    else:
                        skip_cnt += 1
                        lines.append(f"- {os.path.basename(item['file'])}：未发现符合规则的图片（跳过）")
                summary = f"处理完成：成功 {success_cnt} 个，未变更 {skip_cnt} 个\n\n" + "\n".join(lines[:50])
                if len(self.auto_report) > 50:
                    summary += f"\n... 其余 {len(self.auto_report) - 50} 个文件已省略"
                messagebox.showinfo("批处理报告", summary)
            else:
                messagebox.showinfo("完成", "所有文件已处理完成。")
            return
        current = self.batch_queue.pop(0)
        self.status_var.set(f"分析：{os.path.basename(current)}")
        self.progress['value'] = 0
        self.processor = PdfImageProcessor(current)
        self.runner = AnalysisRunner(self.root, self.processor, self._on_progress, self._on_result_auto, self._on_error)
        self.runner.start(self._rules())

    def _on_result_auto(self, result):
        candidates, _previews = result
        xrefs = set()
        for items in candidates.values():
            for it in items:
                xrefs.add(it['xref'])
        if xrefs:
            try:
                out_path, removed_count, affected_pages = self.file_saver.auto_save(self.processor, xrefs,
                                                                                    self.processor.file_path,
                                                                                    self.output_dir)
                self.status_var.set(f"保存：{os.path.basename(out_path)}（移除 {removed_count} 处，{len(affected_pages)} 页）")
                if hasattr(self, 'auto_report'):
                    self.auto_report.append({
                        'file': self.processor.file_path,
                        'out': out_path,
                        'removed': removed_count,
                        'pages': list(affected_pages),
                    })
            except Exception as e:
                messagebox.showerror("保存失败", f"{os.path.basename(self.processor.file_path)} 保存失败：\n{e}")
                if hasattr(self, 'auto_report'):
                    self.auto_report.append({
                        'file': self.processor.file_path,
                        'out': '',
                        'removed': 0,
                        'pages': [],
                    })
        else:
            if hasattr(self, 'auto_report'):
                self.auto_report.append({
                    'file': self.processor.file_path,
                    'out': '',
                    'removed': 0,
                    'pages': [],
                })
        self._auto_next()

    def _on_progress(self, msg: dict):
        if 'progress' in msg:
            target = float(msg['progress'])
            status = msg.get('status', '正在分析...')

            self.progress_ctl.update(target, status, run_id=getattr(self, 'current_run_id', None))

    def _on_error(self, err: str):
        messagebox.showerror("分析失败", err)
        if self.batch_mode == 'interactive':
            self._interactive_next()
        elif self.batch_mode == 'auto':
            self._auto_next()
        else:
            self._set_running(False, "分析失败")

    def _set_running(self, running: bool, status_text: str = None):

        self.is_running = running
        if status_text is not None:
            self.status_var.set(status_text)
        state = 'disabled' if running else 'normal'

        self.preview_btn.config(state=state if self.file_list.size() > 0 and not running else 'disabled')
        self.auto_btn.config(state=state if self.file_list.size() > 0 and not running else 'disabled')

        for child in self.file_list.master.winfo_children():
            if isinstance(child, ttk.Frame):
                for btn in child.winfo_children():
                    try:
                        btn.config(state=state)
                    except Exception:
                        pass
        try:
            self.file_list.config(state=state)
        except Exception:
            pass
        try:
            for w in self.root.winfo_children():
                if isinstance(w, ttk.Frame) or isinstance(w, ttk.LabelFrame):
                    if str(w.cget('text')) == '移除规则':
                        for sw in w.winfo_children():
                            try:
                                sw.config(state=state)
                            except Exception:
                                pass
        except Exception:
            pass

        if not running:
            self.batch_mode = None
            self.batch_queue = []
            self.output_dir = None


def main():
    root = TkinterDnD.Tk()
    app = MainApp(root)
    root.mainloop()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
