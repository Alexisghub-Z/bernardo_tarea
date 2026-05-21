"""
DSS-VacOaxaca - Sistema de Soporte a Decisiones
Módulo de Recopilación de Datos desde Excel
Instituto Tecnológico de Oaxaca
"""

import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
import ttkbootstrap as ttk_boot
from ttkbootstrap.constants import *
import pandas as pd
import os
import threading
import time
from datetime import datetime
from mongodb_connector import MongoDBConnector
import warnings
warnings.filterwarnings('ignore', message='DataFrame is highly fragmented')

try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    import matplotlib
    matplotlib.rcParams.update({
        'font.family': 'Segoe UI',
        'axes.spines.top': False,
        'axes.spines.right': False,
    })
    MATPLOTLIB_OK = True
except ImportError:
    MATPLOTLIB_OK = False


# ─────────────────────────────────────────────
#  Colores y estilos personalizados
# ─────────────────────────────────────────────
VERDE_SALUD   = "#00875A"
VERDE_CLARO   = "#00C07F"
AZUL_GOB      = "#003A70"
BLANCO        = "#FFFFFF"
GRIS_FONDO    = "#F4F6F9"
GRIS_BORDE    = "#D0D7E3"
NARANJA_ALERTA = "#FF6B2B"
TEXTO_OSCURO  = "#1A2340"
AMARILLO_SUGE = "#FFF9E6"

# ─────────────────────────────────────────────
#  Diccionario de estados mexicanos
#  Mapea abreviaturas y variantes → nombre canónico
# ─────────────────────────────────────────────
ESTADOS_MEXICO = {
    # Aguascalientes
    "ags": "Aguascalientes", "aguascalientes": "Aguascalientes",
    # Baja California
    "bc": "Baja California", "baja california": "Baja California",
    "bajacalifornia": "Baja California",
    # Baja California Sur
    "bcs": "Baja California Sur", "baja california sur": "Baja California Sur",
    "bajacaliforniasur": "Baja California Sur",
    # Campeche
    "camp": "Campeche", "campeche": "Campeche",
    # Chiapas
    "chis": "Chiapas", "chiapas": "Chiapas", "chps": "Chiapas",
    # Chihuahua
    "chih": "Chihuahua", "chihuahua": "Chihuahua",
    # Ciudad de México
    "cdmx": "Ciudad de Mexico", "ciudad de mexico": "Ciudad de Mexico",
    "ciudad de méxico": "Ciudad de Mexico", "df": "Ciudad de Mexico",
    "distrito federal": "Ciudad de Mexico", "distritofederal": "Ciudad de Mexico",
    # Coahuila
    "coah": "Coahuila", "coahuila": "Coahuila",
    "coahuila de zaragoza": "Coahuila",
    # Colima
    "col": "Colima", "colima": "Colima",
    # Durango
    "dgo": "Durango", "durango": "Durango", "dur": "Durango",
    # Guanajuato
    "gto": "Guanajuato", "guanajuato": "Guanajuato",
    # Guerrero
    "gro": "Guerrero", "guerrero": "Guerrero",
    # Hidalgo
    "hgo": "Hidalgo", "hidalgo": "Hidalgo",
    # Jalisco
    "jal": "Jalisco", "jalisco": "Jalisco",
    # Estado de México
    "edomex": "Estado de Mexico", "estado de mexico": "Estado de Mexico",
    "estado de méxico": "Estado de Mexico", "mex": "Estado de Mexico",
    "edo mex": "Estado de Mexico", "edo. mex.": "Estado de Mexico",
    # Michoacán
    "mich": "Michoacan", "michoacan": "Michoacan",
    "michoacán": "Michoacan", "michoacan de ocampo": "Michoacan",
    # Morelos
    "mor": "Morelos", "morelos": "Morelos",
    # Nayarit
    "nay": "Nayarit", "nayarit": "Nayarit",
    # Nuevo León
    "nl": "Nuevo Leon", "nle": "Nuevo Leon", "nuevo leon": "Nuevo Leon",
    "nuevo león": "Nuevo Leon",
    # Oaxaca
    "oax": "Oaxaca", "oaxaca": "Oaxaca",
    # Puebla
    "pue": "Puebla", "puebla": "Puebla",
    # Querétaro
    "qro": "Queretaro", "queretaro": "Queretaro", "querétaro": "Queretaro",
    # Quintana Roo
    "qroo": "Quintana Roo", "quintana roo": "Quintana Roo",
    "quintanaroo": "Quintana Roo",
    # San Luis Potosí
    "slp": "San Luis Potosi", "san luis potosi": "San Luis Potosi",
    "san luis potosí": "San Luis Potosi",
    # Sinaloa
    "sin": "Sinaloa", "sinaloa": "Sinaloa",
    # Sonora
    "son": "Sonora", "sonora": "Sonora",
    # Tabasco
    "tab": "Tabasco", "tabasco": "Tabasco",
    # Tamaulipas
    "tamps": "Tamaulipas", "tamaulipas": "Tamaulipas", "tam": "Tamaulipas",
    # Tlaxcala
    "tlax": "Tlaxcala", "tlaxcala": "Tlaxcala",
    # Veracruz
    "ver": "Veracruz", "veracruz": "Veracruz",
    "veracruz de ignacio de la llave": "Veracruz",
    # Yucatán
    "yuc": "Yucatan", "yucatan": "Yucatan", "yucatán": "Yucatan",
    # Zacatecas
    "zac": "Zacatecas", "zacatecas": "Zacatecas",
}


class DSSVacunacionApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DSS-VacOaxaca | Sistema de Recopilación de Datos")
        self.root.geometry("1100x780")
        self.root.minsize(900, 650)
        self.root.configure(bg=GRIS_FONDO)

        self.df = None
        self.archivo_cargado = None
        self.cols_estructuradas = []
        self.cols_no_estructuradas = []
        self.pantalla_actual = None
        self.archivos_no_estructurados = []
        self.resumen_homogeneizacion = {}
        self.contenido_archivos_externos = {}

        # MongoDB connection
        try:
            self.mongo = MongoDBConnector()
            print("✓ MongoDB conectado exitosamente")
        except Exception as e:
            print(f"⚠ Error al conectar a MongoDB: {e}")
            self.mongo = None

        self._construir_ui()

    # ══════════════════════════════════════════
    #  CONSTRUCCIÓN DE LA INTERFAZ
    # ══════════════════════════════════════════
    def _construir_ui(self):
        # ── Encabezado ──────────────────────────
        encabezado = tk.Frame(self.root, bg=AZUL_GOB, height=75)
        encabezado.pack(fill="x")
        encabezado.pack_propagate(False)

        tk.Label(
            encabezado,
            text="DSS-VacOaxaca 2010-2026",
            font=("Segoe UI", 18, "bold"),
            bg=AZUL_GOB, fg=BLANCO
        ).pack(side="left", padx=20, pady=15)

        tk.Label(
            encabezado,
            text="Sistema de Soporte a Decisiones",
            font=("Segoe UI", 10),
            bg=AZUL_GOB, fg="#A8C4E0"
        ).pack(side="left", padx=5, pady=22)

        self.lbl_reloj = tk.Label(
            encabezado,
            text="",
            font=("Segoe UI", 9),
            bg=AZUL_GOB, fg="#A8C4E0"
        )
        self.lbl_reloj.pack(side="right", padx=20)
        self._actualizar_reloj()

        # ── Barra de navegación ─────────────────
        nav_bar = tk.Frame(self.root, bg="#002952", height=50)
        nav_bar.pack(fill="x")
        nav_bar.pack_propagate(False)

        nav_inner = tk.Frame(nav_bar, bg="#002952")
        nav_inner.pack(expand=True)

        self.btn_nav_extraer = tk.Button(
            nav_inner,
            text="EXTRAER",
            font=("Segoe UI", 11, "bold"),
            bg=VERDE_SALUD, fg=BLANCO,
            relief="flat", cursor="hand2",
            padx=30, pady=8,
            command=lambda: self.mostrar_pantalla("extraer")
        )
        self.btn_nav_extraer.pack(side="left", padx=8, pady=6)

        self.btn_nav_mostrar = tk.Button(
            nav_inner,
            text="MOSTRAR",
            font=("Segoe UI", 11, "bold"),
            bg=AZUL_GOB, fg=BLANCO,
            relief="flat", cursor="hand2",
            padx=30, pady=8,
            command=lambda: self.mostrar_pantalla("mostrar")
        )
        self.btn_nav_mostrar.pack(side="left", padx=8, pady=6)

        self.btn_nav_dashboard = tk.Button(
            nav_inner,
            text="DASHBOARD",
            font=("Segoe UI", 11, "bold"),
            bg=AZUL_GOB, fg=BLANCO,
            relief="flat", cursor="hand2",
            padx=30, pady=8,
            command=lambda: self.mostrar_pantalla("dashboard")
        )
        self.btn_nav_dashboard.pack(side="left", padx=8, pady=6)

        # ── Barra de estado ─────────────────────
        self.status_bar = tk.Frame(self.root, bg=VERDE_SALUD, height=32)
        self.status_bar.pack(fill="x")
        self.status_bar.pack_propagate(False)

        self.lbl_status = tk.Label(
            self.status_bar,
            text="  Listo -- Selecciona un archivo para comenzar",
            font=("Segoe UI", 9),
            bg=VERDE_SALUD, fg=BLANCO, anchor="w"
        )
        self.lbl_status.pack(side="left", fill="x", expand=True, padx=10, pady=5)

        # ── Contenedor de pantallas ─────────────
        self.contenedor = tk.Frame(self.root, bg=GRIS_FONDO)
        self.contenedor.pack(fill="both", expand=True)

        self.pantallas = {}
        self._construir_pantalla_extraer()
        self._construir_pantalla_transformar()
        self._construir_pantalla_mostrar()
        self._construir_pantalla_dashboard()

        # ── Barra inferior ───────────────────────
        barra_inf = tk.Frame(self.root, bg=AZUL_GOB, height=28)
        barra_inf.pack(fill="x", side="bottom")
        barra_inf.pack_propagate(False)

        tk.Label(
            barra_inf,
            text="Instituto Tecnologico de Oaxaca  |  Ingenieria en Sistemas Computacionales  |  Octavo Semestre",
            font=("Segoe UI", 8),
            bg=AZUL_GOB, fg="#A8C4E0"
        ).pack(side="left", padx=15, pady=4)

        # Mostrar pantalla inicial
        self.mostrar_pantalla("extraer")

    def _actualizar_reloj(self):
        self.lbl_reloj.config(text=datetime.now().strftime('%d/%m/%Y  %H:%M:%S'))
        self.root.after(1000, self._actualizar_reloj)

    # ──────────────────────────────────────────
    #  NAVEGACIÓN ENTRE PANTALLAS
    # ──────────────────────────────────────────
    def mostrar_pantalla(self, nombre):
        for key, frame in self.pantallas.items():
            frame.pack_forget()

        self.pantallas[nombre].pack(fill="both", expand=True)
        self.pantalla_actual = nombre

        # Actualizar botones de navegación
        btns = {
            "extraer": self.btn_nav_extraer,
            "mostrar": self.btn_nav_mostrar,
            "dashboard": self.btn_nav_dashboard,
        }
        for key, btn in btns.items():
            btn.config(bg=VERDE_SALUD if key == nombre else AZUL_GOB)

    # ══════════════════════════════════════════
    #  PANTALLA 1: EXTRAER
    # ══════════════════════════════════════════
    def _construir_pantalla_extraer(self):
        frame = tk.Frame(self.contenedor, bg=GRIS_FONDO)
        self.pantallas["extraer"] = frame

        # Centrar contenido
        center = tk.Frame(frame, bg=GRIS_FONDO)
        center.place(relx=0.5, rely=0.45, anchor="center")

        # Título
        tk.Label(
            center,
            text="EXTRAER DATOS",
            font=("Segoe UI", 22, "bold"),
            bg=GRIS_FONDO, fg=AZUL_GOB
        ).pack(pady=(0, 5))

        tk.Label(
            center,
            text="Selecciona un archivo Excel o CSV para extraer la informacion",
            font=("Segoe UI", 11),
            bg=GRIS_FONDO, fg="#7A8499"
        ).pack(pady=(0, 30))

        # Card de selección de archivo
        card = tk.Frame(center, bg=BLANCO, relief="flat",
                        highlightbackground=GRIS_BORDE, highlightthickness=1)
        card.pack(fill="x", padx=40, pady=(0, 20))

        card_inner = tk.Frame(card, bg=BLANCO)
        card_inner.pack(padx=30, pady=25)

        tk.Label(
            card_inner,
            text="Archivo seleccionado:",
            font=("Segoe UI", 9, "bold"),
            bg=BLANCO, fg=TEXTO_OSCURO
        ).pack(anchor="w")

        self.lbl_archivo = tk.Label(
            card_inner,
            text="Ningun archivo seleccionado",
            font=("Segoe UI", 10),
            bg=BLANCO, fg="#7A8499",
            wraplength=400, justify="left", anchor="w"
        )
        self.lbl_archivo.pack(fill="x", pady=(5, 15))

        btn_explorar = tk.Button(
            card_inner,
            text="Explorar archivo...",
            font=("Segoe UI", 10, "bold"),
            bg=AZUL_GOB, fg=BLANCO,
            relief="flat", cursor="hand2",
            padx=20, pady=10,
            command=self.explorar_archivo
        )
        btn_explorar.pack(fill="x")
        self._hover(btn_explorar, AZUL_GOB, "#00529B")

        # Selector de hoja (solo para Excel)
        self.hoja_frame = tk.Frame(card_inner, bg=BLANCO)
        self.hoja_frame.pack(fill="x", pady=(12, 0))

        tk.Label(self.hoja_frame, text="Hoja de Excel:", font=("Segoe UI", 9, "bold"),
                 bg=BLANCO, fg=TEXTO_OSCURO).pack(anchor="w")
        self.combo_hoja = ttk.Combobox(self.hoja_frame, state="readonly",
                                       font=("Segoe UI", 9))
        self.combo_hoja.pack(fill="x", pady=(3, 0))

        # Botón principal EXTRAER
        self.btn_extraer = tk.Button(
            center,
            text="EXTRAER DATOS",
            font=("Segoe UI", 14, "bold"),
            bg=VERDE_SALUD, fg=BLANCO,
            relief="flat", cursor="hand2",
            padx=40, pady=16,
            command=self.ejecutar_extraccion
        )
        self.btn_extraer.pack(pady=(10, 15))
        self._hover(self.btn_extraer, VERDE_SALUD, "#005C3D")

        # Card de importación desde URL
        card_url = tk.Frame(center, bg=BLANCO, relief="flat",
                            highlightbackground=GRIS_BORDE, highlightthickness=1)
        card_url.pack(fill="x", padx=40, pady=(0, 20))

        card_url_inner = tk.Frame(card_url, bg=BLANCO)
        card_url_inner.pack(padx=30, pady=25)

        tk.Label(
            card_url_inner,
            text="Importar desde URL:",
            font=("Segoe UI", 9, "bold"),
            bg=BLANCO, fg=TEXTO_OSCURO
        ).pack(anchor="w")

        tk.Label(
            card_url_inner,
            text="Pega el enlace directo a un archivo CSV o Excel (GitHub, Our World in Data, etc.)",
            font=("Segoe UI", 8),
            bg=BLANCO, fg="#7A8499"
        ).pack(anchor="w", pady=(2, 8))

        self.entry_url = tk.Entry(
            card_url_inner,
            font=("Segoe UI", 9),
            fg="#7A8499", bg="#F5F7FA",
            relief="flat",
            highlightbackground=GRIS_BORDE, highlightthickness=1
        )
        self.entry_url.insert(0, "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/country_data/Mexico.csv")
        self.entry_url.pack(fill="x", pady=(0, 10), ipady=7)
        self.entry_url.bind("<FocusIn>", lambda e: (
            self.entry_url.delete(0, "end"),
            self.entry_url.config(fg=TEXTO_OSCURO)
        ) if self.entry_url.get().startswith("https://raw.github") else None)

        btn_url = tk.Button(
            card_url_inner,
            text="Descargar y usar este archivo",
            font=("Segoe UI", 10, "bold"),
            bg=AZUL_GOB, fg=BLANCO,
            relief="flat", cursor="hand2",
            padx=20, pady=10,
            command=self.descargar_desde_url
        )
        btn_url.pack(fill="x")
        self._hover(btn_url, AZUL_GOB, "#00529B")

        self.lbl_url_estado = tk.Label(
            card_url_inner,
            text="",
            font=("Segoe UI", 8),
            bg=BLANCO, fg="#7A8499",
            wraplength=400, justify="left", anchor="w"
        )
        self.lbl_url_estado.pack(fill="x", pady=(6, 0))

        # Card de archivos no estructurados adicionales
        card_ne = tk.Frame(center, bg=BLANCO, relief="flat",
                           highlightbackground=GRIS_BORDE, highlightthickness=1)
        card_ne.pack(fill="x", padx=40, pady=(0, 15))

        card_ne_inner = tk.Frame(card_ne, bg=BLANCO)
        card_ne_inner.pack(padx=30, pady=15)

        tk.Label(
            card_ne_inner,
            text="Archivos no estructurados adicionales (opcional):",
            font=("Segoe UI", 9, "bold"),
            bg=BLANCO, fg=TEXTO_OSCURO
        ).pack(anchor="w")

        tk.Label(
            card_ne_inner,
            text="Agrega archivos TXT, JSON o PDF como fuentes complementarias",
            font=("Segoe UI", 8),
            bg=BLANCO, fg="#7A8499"
        ).pack(anchor="w", pady=(2, 8))

        btn_ne = tk.Button(
            card_ne_inner,
            text="Agregar archivos no estructurados...",
            font=("Segoe UI", 9),
            bg="#E8ECF3", fg=TEXTO_OSCURO,
            relief="flat", cursor="hand2",
            padx=15, pady=6,
            command=self.explorar_archivos_no_estructurados
        )
        btn_ne.pack(fill="x")
        self._hover(btn_ne, "#E8ECF3", "#D0D7E3")

        self.lbl_archivos_ne = tk.Label(
            card_ne_inner,
            text="Ningun archivo adicional seleccionado",
            font=("Segoe UI", 8),
            bg=BLANCO, fg="#7A8499",
            wraplength=400, justify="left", anchor="w"
        )
        self.lbl_archivos_ne.pack(fill="x", pady=(5, 0))

        # Barra de progreso
        self.progreso_extraer = ttk.Progressbar(center, mode="indeterminate",
                                                length=420,
                                                style="success.Horizontal.TProgressbar")
        self.progreso_extraer.pack(pady=(0, 8))

        self.lbl_resumen_extraccion = tk.Label(
            center, text="",
            font=("Segoe UI", 9), bg=GRIS_FONDO, fg="#7A8499"
        )
        self.lbl_resumen_extraccion.pack()

    # ══════════════════════════════════════════
    #  PANTALLA 2: TRANSFORMAR (pantalla de carga)
    # ══════════════════════════════════════════
    def _construir_pantalla_transformar(self):
        frame = tk.Frame(self.contenedor, bg=GRIS_FONDO)
        self.pantallas["transformar"] = frame

        center = tk.Frame(frame, bg=GRIS_FONDO)
        center.place(relx=0.5, rely=0.42, anchor="center")

        # Ícono / indicador visual
        tk.Label(center, text="⚙", font=("Segoe UI", 40),
                 bg=GRIS_FONDO, fg=AZUL_GOB).pack(pady=(0, 12))

        tk.Label(center, text="Procesando datos",
                 font=("Segoe UI", 20, "bold"),
                 bg=GRIS_FONDO, fg=AZUL_GOB).pack()

        tk.Label(center, text="Esto tomará solo unos segundos...",
                 font=("Segoe UI", 10),
                 bg=GRIS_FONDO, fg="#B0B8C9").pack(pady=(4, 28))

        # Barra de progreso
        self.progreso_transformar = ttk.Progressbar(
            center, mode="determinate", length=460,
            style="success.Horizontal.TProgressbar"
        )
        self.progreso_transformar.pack(pady=(0, 14))

        # Paso actual — una sola línea, limpia
        self.lbl_paso_transformar = tk.Label(
            center, text="",
            font=("Segoe UI", 10), bg=GRIS_FONDO, fg=VERDE_SALUD
        )
        self.lbl_paso_transformar.pack()

        # Pasos como chips visuales (se pintan dinámicamente)
        self._chips_frame = tk.Frame(center, bg=GRIS_FONDO)
        self._chips_frame.pack(pady=(20, 0))
        self._chips = []
        pasos_labels = ["Extraer", "Clasificar", "Homogeneizar",
                        "Cargar ext.", "Limpiar", "Listo"]
        for i, p in enumerate(pasos_labels):
            chip = tk.Label(self._chips_frame, text=p,
                            font=("Segoe UI", 8), bg="#E8ECF3",
                            fg="#B0B8C9", padx=10, pady=4,
                            relief="flat")
            chip.grid(row=0, column=i, padx=3)
            self._chips.append(chip)

    def _activar_chip(self, idx):
        """Colorea el chip del paso idx como activo."""
        for i, chip in enumerate(self._chips):
            if i < idx:
                chip.config(bg=VERDE_SALUD, fg=BLANCO)
            elif i == idx:
                chip.config(bg=AZUL_GOB, fg=BLANCO)
            else:
                chip.config(bg="#E8ECF3", fg="#B0B8C9")

    # ══════════════════════════════════════════
    #  PANTALLA 3: MOSTRAR
    # ══════════════════════════════════════════
    def _construir_pantalla_mostrar(self):
        frame = tk.Frame(self.contenedor, bg=GRIS_FONDO)
        self.pantallas["mostrar"] = frame

        # ── Tabla principal (80% del espacio) ──
        card_tabla = tk.Frame(frame, bg=BLANCO, relief="flat",
                              highlightbackground=GRIS_BORDE, highlightthickness=1)
        card_tabla.pack(fill="both", expand=True, padx=15, pady=(10, 4))

        # Cabecera de la tabla
        cab = tk.Frame(card_tabla, bg=BLANCO)
        cab.pack(fill="x", padx=16, pady=(12, 0))

        self.lbl_titulo_tabla = tk.Label(
            cab, text="Datos",
            font=("Segoe UI", 13, "bold"), bg=BLANCO, fg=TEXTO_OSCURO
        )
        self.lbl_titulo_tabla.pack(side="left")

        self.lbl_conteo = tk.Label(
            cab, text="",
            font=("Segoe UI", 9), bg=BLANCO, fg="#7A8499"
        )
        self.lbl_conteo.pack(side="right", pady=2)

        # Stats inline debajo del título
        self.lbl_stats = tk.Label(
            card_tabla, text="",
            font=("Segoe UI", 8), bg=BLANCO, fg="#B0B8C9", anchor="w"
        )
        self.lbl_stats.pack(fill="x", padx=16, pady=(0, 6))

        # Toolbar: buscar + botones
        toolbar = tk.Frame(card_tabla, bg="#F8FAFC")
        toolbar.pack(fill="x")

        tk.Frame(card_tabla, bg=GRIS_BORDE, height=1).pack(fill="x")

        inner_tb = tk.Frame(toolbar, bg="#F8FAFC")
        inner_tb.pack(fill="x", padx=12, pady=6)

        # Búsqueda
        buscar_wrap = tk.Frame(inner_tb, bg="#F8FAFC",
                               highlightbackground=GRIS_BORDE, highlightthickness=1)
        buscar_wrap.pack(side="left")
        tk.Label(buscar_wrap, text=" 🔍 ", font=("Segoe UI", 9),
                 bg="#F8FAFC", fg="#7A8499").pack(side="left")
        self.entry_buscar = tk.Entry(buscar_wrap, font=("Segoe UI", 9),
                                     relief="flat", bd=0, width=28,
                                     bg="#F8FAFC", fg=TEXTO_OSCURO)
        self.entry_buscar.pack(side="left", pady=5, padx=(0, 6))
        self.entry_buscar.bind("<KeyRelease>", self.filtrar_datos)

        btn_limpiar = tk.Button(
            inner_tb, text="✕",
            font=("Segoe UI", 9), bg="#F8FAFC", fg="#7A8499",
            relief="flat", cursor="hand2", padx=6, pady=4,
            command=self.limpiar_filtros
        )
        btn_limpiar.pack(side="left", padx=(6, 0))

        btn_csv = tk.Button(
            inner_tb, text="↓  CSV",
            font=("Segoe UI", 9, "bold"), bg=VERDE_SALUD, fg=BLANCO,
            relief="flat", cursor="hand2", padx=12, pady=4,
            command=self.exportar_csv
        )
        btn_csv.pack(side="right", padx=(5, 0))
        self._hover(btn_csv, VERDE_SALUD, "#005C3D")

        btn_reporte = tk.Button(
            inner_tb, text="↓  TXT",
            font=("Segoe UI", 9, "bold"), bg=AZUL_GOB, fg=BLANCO,
            relief="flat", cursor="hand2", padx=12, pady=4,
            command=self.generar_reporte
        )
        btn_reporte.pack(side="right", padx=(5, 0))
        self._hover(btn_reporte, AZUL_GOB, "#00529B")

        # Tabla
        tabla_frame = tk.Frame(card_tabla, bg=BLANCO)
        tabla_frame.pack(fill="both", expand=True, padx=0, pady=0)

        scroll_y = ttk.Scrollbar(tabla_frame, orient="vertical")
        scroll_y.pack(side="right", fill="y")
        scroll_x = ttk.Scrollbar(tabla_frame, orient="horizontal")
        scroll_x.pack(side="bottom", fill="x")

        self.tabla = ttk.Treeview(
            tabla_frame,
            yscrollcommand=scroll_y.set,
            xscrollcommand=scroll_x.set,
            style="Custom.Treeview"
        )
        self.tabla.pack(fill="both", expand=True)

        scroll_y.config(command=self.tabla.yview)
        scroll_x.config(command=self.tabla.xview)

        estilo = ttk.Style()
        estilo.configure("Custom.Treeview",
                         background=BLANCO, foreground=TEXTO_OSCURO,
                         rowheight=26, fieldbackground=BLANCO,
                         font=("Segoe UI", 9))
        estilo.configure("Custom.Treeview.Heading",
                         background=AZUL_GOB, foreground=BLANCO,
                         font=("Segoe UI", 9, "bold"), relief="flat")
        estilo.map("Custom.Treeview",
                   background=[("selected", VERDE_CLARO)],
                   foreground=[("selected", BLANCO)])

        self._mostrar_placeholder()

        # ── Sección colapsable: texto no estructurado ──
        self._ne_visible = False
        ne_toggle_bar = tk.Frame(frame, bg=GRIS_FONDO)
        ne_toggle_bar.pack(fill="x", padx=15, pady=(0, 2))

        self._btn_toggle_ne = tk.Button(
            ne_toggle_bar,
            text="▶  Datos no estructurados",
            font=("Segoe UI", 8, "bold"), bg=GRIS_FONDO, fg="#7A8499",
            relief="flat", cursor="hand2", anchor="w",
            command=self._toggle_no_estructurado
        )
        self._btn_toggle_ne.pack(side="left")

        self.lbl_conteo_no_estruct = tk.Label(
            ne_toggle_bar, text="",
            font=("Segoe UI", 8), bg=GRIS_FONDO, fg="#B0B8C9"
        )
        self.lbl_conteo_no_estruct.pack(side="left", padx=(6, 0))

        self._ne_frame = tk.Frame(frame, bg=BLANCO, relief="flat",
                                  highlightbackground=GRIS_BORDE, highlightthickness=1)

        self.lbl_titulo_no_estruct = tk.Label(
            self._ne_frame, text="",
            font=("Segoe UI", 10, "bold"), bg=BLANCO, fg=TEXTO_OSCURO
        )

        self.txt_no_estructurado = scrolledtext.ScrolledText(
            self._ne_frame,
            font=("Consolas", 8), bg="#FAFBFC", fg=TEXTO_OSCURO,
            wrap="word", relief="flat", state="disabled", height=8
        )
        self.txt_no_estructurado.pack(fill="both", expand=True, padx=12, pady=8)

    def _toggle_no_estructurado(self):
        self._ne_visible = not self._ne_visible
        if self._ne_visible:
            self._ne_frame.pack(fill="both", padx=15, pady=(0, 8))
            self._btn_toggle_ne.config(text="▼  Datos no estructurados")
        else:
            self._ne_frame.pack_forget()
            self._btn_toggle_ne.config(text="▶  Datos no estructurados")

    # ══════════════════════════════════════════
    #  PANTALLA 4: DASHBOARD DE KPIs
    # ══════════════════════════════════════════
    def _construir_pantalla_dashboard(self):
        frame = tk.Frame(self.contenedor, bg=GRIS_FONDO)
        self.pantallas["dashboard"] = frame

        # Header con título y botón actualizar
        header = tk.Frame(frame, bg=GRIS_FONDO)
        header.pack(fill="x", padx=15, pady=(10, 0))

        tk.Label(
            header,
            text="DASHBOARD DE KPIs",
            font=("Segoe UI", 18, "bold"),
            bg=GRIS_FONDO, fg=AZUL_GOB
        ).pack(side="left")

        tk.Label(
            header,
            text="Indicadores clave calculados a partir de los datos cargados",
            font=("Segoe UI", 9),
            bg=GRIS_FONDO, fg="#7A8499"
        ).pack(side="left", padx=(12, 0), pady=4)

        btn_act = tk.Button(
            header,
            text="↻  Actualizar",
            font=("Segoe UI", 10, "bold"),
            bg=VERDE_SALUD, fg=BLANCO,
            relief="flat", cursor="hand2",
            padx=15, pady=6,
            command=self.actualizar_dashboard
        )
        btn_act.pack(side="right")
        self._hover(btn_act, VERDE_SALUD, "#005C3D")

        sep = tk.Frame(frame, bg=GRIS_BORDE, height=1)
        sep.pack(fill="x", padx=15, pady=(8, 0))

        # ── Barra de filtros ─────────────────────
        filtros_frame = tk.Frame(frame, bg=GRIS_FONDO)
        filtros_frame.pack(fill="x", padx=15, pady=(6, 2))

        tk.Label(filtros_frame, text="Filtrar por:", font=("Segoe UI", 9, "bold"),
                 bg=GRIS_FONDO, fg=TEXTO_OSCURO).pack(side="left", padx=(0, 6))

        tk.Label(filtros_frame, text="Estado:", font=("Segoe UI", 9),
                 bg=GRIS_FONDO, fg="#7A8499").pack(side="left")
        self._dash_combo_estado = ttk.Combobox(
            filtros_frame, state="readonly", width=18,
            font=("Segoe UI", 9)
        )
        self._dash_combo_estado.pack(side="left", padx=(2, 10))

        tk.Label(filtros_frame, text="Municipio:", font=("Segoe UI", 9),
                 bg=GRIS_FONDO, fg="#7A8499").pack(side="left")
        self._dash_combo_mun = ttk.Combobox(
            filtros_frame, state="readonly", width=20,
            font=("Segoe UI", 9)
        )
        self._dash_combo_mun.pack(side="left", padx=(2, 10))

        tk.Label(filtros_frame, text="Mes:", font=("Segoe UI", 9),
                 bg=GRIS_FONDO, fg="#7A8499").pack(side="left")
        self._dash_combo_mes = ttk.Combobox(
            filtros_frame, state="readonly", width=10,
            font=("Segoe UI", 9),
            values=["Todos", "Ene", "Feb", "Mar", "Abr", "May", "Jun",
                    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
        )
        self._dash_combo_mes.current(0)
        self._dash_combo_mes.pack(side="left", padx=(2, 10))

        btn_filtrar = tk.Button(
            filtros_frame, text="Aplicar filtros",
            font=("Segoe UI", 9, "bold"),
            bg=AZUL_GOB, fg=BLANCO, relief="flat", cursor="hand2",
            padx=10, pady=3,
            command=self.actualizar_dashboard
        )
        btn_filtrar.pack(side="left", padx=(0, 6))
        self._hover(btn_filtrar, AZUL_GOB, "#0A3055")

        btn_limpiar = tk.Button(
            filtros_frame, text="Limpiar",
            font=("Segoe UI", 9),
            bg="#E8ECF3", fg=TEXTO_OSCURO, relief="flat", cursor="hand2",
            padx=8, pady=3,
            command=self._limpiar_filtros_dashboard
        )
        btn_limpiar.pack(side="left")

        sep2 = tk.Frame(frame, bg=GRIS_BORDE, height=1)
        sep2.pack(fill="x", padx=15, pady=(4, 0))

        # Canvas scrollable
        canvas_frame = tk.Frame(frame, bg=GRIS_FONDO)
        canvas_frame.pack(fill="both", expand=True, padx=15, pady=(6, 10))

        self._dash_canvas = tk.Canvas(canvas_frame, bg=GRIS_FONDO, highlightthickness=0)
        scrollbar_d = ttk.Scrollbar(canvas_frame, orient="vertical",
                                    command=self._dash_canvas.yview)
        self._dash_inner = tk.Frame(self._dash_canvas, bg=GRIS_FONDO)

        self._dash_inner.bind(
            "<Configure>",
            lambda e: self._dash_canvas.configure(
                scrollregion=self._dash_canvas.bbox("all"))
        )
        self._dash_win = self._dash_canvas.create_window(
            (0, 0), window=self._dash_inner, anchor="nw"
        )
        self._dash_canvas.bind(
            "<Configure>",
            lambda e: self._dash_canvas.itemconfig(self._dash_win, width=e.width)
        )
        self._dash_canvas.configure(yscrollcommand=scrollbar_d.set)
        self._dash_canvas.bind_all(
            "<MouseWheel>",
            lambda e: self._dash_canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        )

        scrollbar_d.pack(side="right", fill="y")
        self._dash_canvas.pack(side="left", fill="both", expand=True)

        # Placeholder inicial
        self._dash_placeholder = tk.Label(
            self._dash_inner,
            text="Carga y extrae un archivo para calcular los KPIs",
            font=("Segoe UI", 13),
            bg=GRIS_FONDO, fg="#B0B8C9"
        )
        self._dash_placeholder.pack(pady=100)

    def _limpiar_filtros_dashboard(self):
        if hasattr(self, '_dash_combo_estado') and self._dash_combo_estado['values']:
            self._dash_combo_estado.current(0)
        if hasattr(self, '_dash_combo_mun') and self._dash_combo_mun['values']:
            self._dash_combo_mun.current(0)
        if hasattr(self, '_dash_combo_mes'):
            self._dash_combo_mes.current(0)
        self.actualizar_dashboard()

    def _poblar_combos_filtros(self, df):
        """Llena los combos de estado y municipio con los valores únicos del df."""
        NO_DOSIS = {"CLAVE_ENTIDAD", "ENTIDAD", "CLAVE_MUNICIPIO", "MUNICIPIO",
                    "CLUES", "NOMBRE_CLUES", "MES", "ANIO", "FECHA", "_hoja"}
        def col_exacta(nombre):
            for c in df.columns:
                if c.upper() == nombre.upper():
                    return c
            return None

        c_ent = col_exacta("ENTIDAD")
        c_mun = col_exacta("MUNICIPIO")

        if hasattr(self, '_dash_combo_estado'):
            estados = ["Todos"] + (sorted(df[c_ent].dropna().unique().tolist())
                                   if c_ent else [])
            sel = self._dash_combo_estado.get()
            self._dash_combo_estado["values"] = estados
            if sel in estados:
                self._dash_combo_estado.set(sel)
            else:
                self._dash_combo_estado.current(0)

        if hasattr(self, '_dash_combo_mun'):
            muns = ["Todos"] + (sorted(df[c_mun].dropna().unique().tolist())
                                if c_mun else [])
            sel = self._dash_combo_mun.get()
            self._dash_combo_mun["values"] = muns
            if sel in muns:
                self._dash_combo_mun.set(sel)
            else:
                self._dash_combo_mun.current(0)

    def _df_filtrado(self):
        """Devuelve el DataFrame filtrado según los combos activos."""
        df = self.df.copy()
        def col_exacta(nombre):
            for c in df.columns:
                if c.upper() == nombre.upper():
                    return c
            return None

        estado_sel = getattr(self, '_dash_combo_estado', None)
        mun_sel    = getattr(self, '_dash_combo_mun', None)
        mes_sel    = getattr(self, '_dash_combo_mes', None)

        NOMBRES_MES = ["Ene","Feb","Mar","Abr","May","Jun",
                       "Jul","Ago","Sep","Oct","Nov","Dic"]

        if estado_sel and estado_sel.get() not in ("Todos", ""):
            c = col_exacta("ENTIDAD")
            if c:
                df = df[df[c] == estado_sel.get()]

        if mun_sel and mun_sel.get() not in ("Todos", ""):
            c = col_exacta("MUNICIPIO")
            if c:
                df = df[df[c] == mun_sel.get()]

        if mes_sel and mes_sel.get() not in ("Todos", ""):
            mes_num = NOMBRES_MES.index(mes_sel.get()) + 1
            c = col_exacta("MES")
            if c:
                df[c] = pd.to_numeric(df[c], errors="coerce")
                df = df[df[c] == mes_num]

        return df

    # ──────────────────────────────────────────
    #  ACTUALIZAR DASHBOARD
    # ──────────────────────────────────────────
    def actualizar_dashboard(self):
        if MATPLOTLIB_OK:
            import matplotlib.pyplot as plt
            for fig in getattr(self, '_dash_figures', []):
                plt.close(fig)
        self._dash_figures = []
        for w in self._dash_inner.winfo_children():
            w.destroy()

        if self.df is None or self.df.empty:
            tk.Label(
                self._dash_inner,
                text="Sin datos cargados. Extrae un archivo primero.",
                font=("Segoe UI", 13), bg=GRIS_FONDO, fg="#B0B8C9"
            ).pack(pady=100)
            return

        self._poblar_combos_filtros(self.df)
        kpis = self._computar_kpis()

        DEFS = [
            {
                "n": 1, "titulo": "Total de Dosis Aplicadas",
                "key": "total_dosis", "unidad": "",
                "hint": "suma de columnas VAC / BIO / BIE / V*",
                "umbrales": [(1_000_000, None, "#00875A", "Alto volumen"),
                             (100_000, 1_000_000, "#F59E0B", "Volumen medio"),
                             (0, 100_000,  "#EF4444", "Bajo volumen")],
                "mayor_mejor": True,
            },
            {
                "n": 2, "titulo": "Entidades Registradas",
                "key": "entidades", "unidad": " estados",
                "hint": "columna ENTIDAD",
                "umbrales": [(20, None, "#00875A", "Alta cobertura"),
                             (10, 20,  "#F59E0B", "Cobertura media"),
                             (0,  10,  "#EF4444", "Baja cobertura")],
                "mayor_mejor": True,
            },
            {
                "n": 3, "titulo": "Municipios Cubiertos",
                "key": "municipios", "unidad": " municipios",
                "hint": "columna MUNICIPIO",
                "umbrales": [(50, None, "#00875A", "Amplia cobertura"),
                             (20, 50,  "#F59E0B", "Cobertura parcial"),
                             (0,  20,  "#EF4444", "Cobertura reducida")],
                "mayor_mejor": True,
            },
            {
                "n": 4, "titulo": "Establecimientos Activos",
                "key": "clues_activos", "unidad": " establecimientos",
                "hint": "columna CLUES (clave interna)",
                "umbrales": [(100, None, "#00875A", "Red amplia"),
                             (30,  100, "#F59E0B", "Red media"),
                             (0,   30,  "#EF4444", "Red reducida")],
                "mayor_mejor": True,
            },
            {
                "n": 5, "titulo": "Dosis BCG Aplicadas",
                "key": "dosis_bcg", "unidad": "",
                "hint": "columnas VBC01, VBC02, VBC03",
                "umbrales": [(50_000, None, "#00875A", "Alto volumen"),
                             (5_000, 50_000,  "#F59E0B", "Volumen medio"),
                             (0,     5_000,   "#EF4444", "Bajo volumen")],
                "mayor_mejor": True,
            },
            {
                "n": 6, "titulo": "Dosis Hepatitis B",
                "key": "dosis_hepb", "unidad": "",
                "hint": "columnas VHB01 – VHB06",
                "umbrales": [(50_000, None, "#00875A", "Alto volumen"),
                             (5_000, 50_000,  "#F59E0B", "Volumen medio"),
                             (0,     5_000,   "#EF4444", "Bajo volumen")],
                "mayor_mejor": True,
            },
            {
                "n": 7, "titulo": "Dosis VPH (Papiloma)",
                "key": "dosis_vph", "unidad": "",
                "hint": "columnas VPH01 – VPH04",
                "umbrales": [(10_000, None, "#00875A", "Alto volumen"),
                             (1_000, 10_000,  "#F59E0B", "Volumen medio"),
                             (0,     1_000,   "#EF4444", "Bajo volumen")],
                "mayor_mejor": True,
            },
            {
                "n": 8, "titulo": "Dosis Rotavirus",
                "key": "dosis_rotavirus", "unidad": "",
                "hint": "columnas VRV01 – VRV04",
                "umbrales": [(50_000, None, "#00875A", "Alto volumen"),
                             (5_000, 50_000,  "#F59E0B", "Volumen medio"),
                             (0,     5_000,   "#EF4444", "Bajo volumen")],
                "mayor_mejor": True,
            },
            {
                "n": 9, "titulo": "Meses con Registros",
                "key": "meses_activos", "unidad": " / 12 meses",
                "hint": "columna MES",
                "umbrales": [(12, None, "#00875A", "Año completo"),
                             (6,  12,  "#F59E0B", "Año parcial"),
                             (0,   6,  "#EF4444", "Datos incompletos")],
                "mayor_mejor": True,
            },
            {
                "n": 10, "titulo": "Promedio Dosis por Establecimiento",
                "key": "promedio_clues", "unidad": " dosis",
                "hint": "total_dosis / n_establecimientos",
                "umbrales": [(500, None, "#00875A", "Alta actividad"),
                             (100, 500,  "#F59E0B", "Actividad media"),
                             (0,   100,  "#EF4444", "Baja actividad")],
                "mayor_mejor": True,
            },
            {
                "n": 11, "titulo": "Top Municipios por Volumen",
                "key": "top_municipios", "unidad": " dosis (1er lugar)",
                "hint": "ranking municipios por total de dosis",
                "umbrales": [(500_000, None, "#7C3AED", "Muy alto"),
                             (50_000, 500_000, "#F59E0B", "Alto"),
                             (0,      50_000,  "#EF4444", "Moderado")],
                "mayor_mejor": True,
            },
            {
                "n": 12, "titulo": "Crecimiento Mensual de Dosis",
                "key": "crecimiento_mensual", "unidad": "%",
                "hint": "variación % del último mes vs. mes anterior",
                "umbrales": [(5,    None, "#00875A", "Crecimiento"),
                             (-5,   5,   "#F59E0B", "Estable"),
                             (-9999,-5,  "#EF4444", "Decremento")],
                "mayor_mejor": True,
            },
            {
                "n": 13, "titulo": "Cobertura Municipal Activa",
                "key": "cobertura_municipal", "unidad": "%",
                "hint": "municipios con ≥1 dosis / total municipios",
                "umbrales": [(75, None, "#00875A", "Alta cobertura"),
                             (40, 75,  "#F59E0B", "Cobertura media"),
                             (0,  40,  "#EF4444", "Baja cobertura")],
                "mayor_mejor": True,
            },
            {
                "n": 14, "titulo": "Vacuna Más Demandada",
                "key": "vacuna_top", "unidad": " dosis",
                "hint": "vacuna con mayor total acumulado",
                "umbrales": [(500_000, None, "#0EA5E9", "Alta demanda"),
                             (50_000, 500_000, "#F59E0B", "Demanda media"),
                             (0,      50_000,  "#EF4444", "Baja demanda")],
                "mayor_mejor": True,
            },
        ]

        # Configurar columnas del grid
        self._dash_inner.columnconfigure(0, weight=1, uniform="col")
        self._dash_inner.columnconfigure(1, weight=1, uniform="col")

        for idx, defn in enumerate(DEFS):
            datos = kpis.get(defn["key"], {})
            valor     = datos.get("valor")
            col_usada = datos.get("cols")
            extra     = datos.get("extra", "")
            defn["chart_data"] = datos.get("chart")
            fila    = idx // 2
            columna = idx % 2
            self._crear_card_kpi(
                self._dash_inner, defn, valor, col_usada, extra, fila, columna
            )

    def _computar_kpis(self):
        df = self._df_filtrado()
        resultado = {}

        NO_DOSIS = {
            "CLAVE_ENTIDAD", "ENTIDAD", "CLAVE_MUNICIPIO", "MUNICIPIO",
            "CLUES", "NOMBRE_CLUES", "MES", "ANIO", "FECHA", "_hoja"
        }
        cols_dosis = [
            c for c in df.select_dtypes(include="number").columns
            if c.upper() not in NO_DOSIS
        ]

        def col_exacta(nombre):
            for c in df.columns:
                if c.upper() == nombre.upper():
                    return c
            return None

        def cols_prefijo(prefijo):
            p = prefijo.upper()
            return [c for c in df.columns if c.upper().startswith(p)]

        def suma_cols(lista):
            if not lista:
                return None
            total = df[lista].apply(pd.to_numeric, errors="coerce").sum().sum()
            return int(total) if total == total else None

        def serie_por_mes(lista_cols):
            """Devuelve dict {mes: total} para los meses 1-12."""
            c_mes = col_exacta("MES")
            if not c_mes or not lista_cols:
                return {}
            tmp = df[[c_mes] + lista_cols].copy()
            tmp[c_mes] = pd.to_numeric(tmp[c_mes], errors="coerce")
            for c in lista_cols:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
            tmp["_total"] = tmp[lista_cols].sum(axis=1)
            agrup = tmp.groupby(c_mes)["_total"].sum()
            return {int(m): int(v) for m, v in agrup.items() if 1 <= int(m) <= 12}

        c_entidad = col_exacta("ENTIDAD")
        c_mun     = col_exacta("MUNICIPIO")
        c_clues   = col_exacta("CLUES")
        c_mes     = col_exacta("MES")

        NOMBRES_MES = ["Ene","Feb","Mar","Abr","May","Jun",
                       "Jul","Ago","Sep","Oct","Nov","Dic"]

        # ── KPI 1: Total de dosis — hbar top-5 entidades ──
        total_dosis = suma_cols(cols_dosis)
        chart1 = None
        if c_entidad and cols_dosis and total_dosis:
            tmp = df[[c_entidad] + cols_dosis].copy()
            for c in cols_dosis:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
            top = (tmp.groupby(c_entidad)[cols_dosis]
                      .sum().sum(axis=1)
                      .nlargest(5).sort_values())
            chart1 = {"type": "hbar", "labels": list(top.index),
                      "values": [int(v) for v in top.values]}
        resultado["total_dosis"] = {
            "valor": total_dosis,
            "cols": f"{len(cols_dosis)} columnas de dosis",
            "chart": chart1,
        }

        # ── KPI 2: Entidades — donut X/32 ──
        n_entidades = int(df[c_entidad].dropna().nunique()) if c_entidad else None
        resultado["entidades"] = {
            "valor": n_entidades,
            "cols": c_entidad,
            "chart": {"type": "donut", "value": n_entidades, "total": 32,
                      "label": "/ 32 estados"} if n_entidades else None,
        }

        # ── KPI 3: Municipios — donut X / total_en_dataset ──
        n_mun = int(df[c_mun].dropna().nunique()) if c_mun else None
        resultado["municipios"] = {
            "valor": n_mun,
            "cols": c_mun,
            "chart": {"type": "donut", "value": n_mun, "total": 2469,
                      "label": "/ 2,469 mun."} if n_mun else None,
        }

        # ── KPI 4: Establecimientos activos — donut activos/total ──
        chart4 = None
        if c_clues and cols_dosis:
            df_num = df[cols_dosis].apply(pd.to_numeric, errors="coerce")
            activos     = int(df.loc[df_num.sum(axis=1) > 0, c_clues].nunique())
            total_clues = int(df[c_clues].nunique())
            chart4 = {"type": "donut", "value": activos, "total": total_clues,
                      "label": f"/ {total_clues} establecimientos"}
            resultado["clues_activos"] = {
                "valor": activos, "cols": c_clues,
                "extra": f"de {total_clues} establecimientos totales",
                "chart": chart4,
            }
        else:
            resultado["clues_activos"] = {"valor": None, "cols": None, "chart": None}

        # ── KPI 5: BCG — barras por mes ──
        vbc = cols_prefijo("VBC")
        serie_bcg = serie_por_mes(vbc)
        resultado["dosis_bcg"] = {
            "valor": suma_cols(vbc),
            "cols": ", ".join(vbc) if vbc else None,
            "chart": {
                "type": "bar",
                "labels": NOMBRES_MES,
                "values": [serie_bcg.get(m, 0) for m in range(1, 13)],
            } if serie_bcg else None,
        }

        # ── KPI 6: Hepatitis B — barras por mes ──
        vhb = cols_prefijo("VHB")
        serie_hb = serie_por_mes(vhb)
        resultado["dosis_hepb"] = {
            "valor": suma_cols(vhb),
            "cols": ", ".join(vhb) if vhb else None,
            "chart": {
                "type": "bar",
                "labels": NOMBRES_MES,
                "values": [serie_hb.get(m, 0) for m in range(1, 13)],
            } if serie_hb else None,
        }

        # ── KPI 7: VPH — barras por mes ──
        vph = cols_prefijo("VPH")
        serie_vph = serie_por_mes(vph)
        resultado["dosis_vph"] = {
            "valor": suma_cols(vph),
            "cols": ", ".join(vph) if vph else None,
            "chart": {
                "type": "bar",
                "labels": NOMBRES_MES,
                "values": [serie_vph.get(m, 0) for m in range(1, 13)],
            } if serie_vph else None,
        }

        # ── KPI 8: Rotavirus — barras por mes ──
        vrv = cols_prefijo("VRV")
        serie_rv = serie_por_mes(vrv)
        resultado["dosis_rotavirus"] = {
            "valor": suma_cols(vrv),
            "cols": ", ".join(vrv) if vrv else None,
            "chart": {
                "type": "bar",
                "labels": NOMBRES_MES,
                "values": [serie_rv.get(m, 0) for m in range(1, 13)],
            } if serie_rv else None,
        }

        # ── KPI 9: Meses con registros — barras todas las vacunas por mes ──
        serie_total = serie_por_mes(cols_dosis)
        n_meses = int(df[c_mes].dropna().nunique()) if c_mes else None
        c_anio = col_exacta("ANIO")
        n_anios = int(df[c_anio].dropna().nunique()) if c_anio else 1
        resultado["meses_activos"] = {
            "valor": n_meses,
            "cols": c_mes,
            "extra": f"en {n_anios} año(s) de datos",
            "chart": {
                "type": "bar",
                "labels": NOMBRES_MES,
                "values": [serie_total.get(m, 0) for m in range(1, 13)],
            } if serie_total else None,
        }

        # ── KPI 10: Promedio por CLUES — hbar top-5 CLUES ──
        chart10 = None
        if c_clues and cols_dosis and total_dosis:
            n_clues = int(df[c_clues].nunique())
            promedio = round(total_dosis / n_clues, 1) if n_clues > 0 else None
            tmp = df[[c_clues] + cols_dosis].copy()
            for c in cols_dosis:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
            top5 = (tmp.groupby(c_clues)[cols_dosis]
                       .sum().sum(axis=1)
                       .nlargest(5).sort_values())
            # Truncar nombre CLUES a 12 caracteres
            labels = [str(k)[:12] for k in top5.index]
            chart10 = {"type": "hbar", "labels": labels,
                       "values": [int(v) for v in top5.values]}
            resultado["promedio_clues"] = {
                "valor": promedio,
                "cols": f"total / {n_clues} establecimientos",
                "chart": chart10,
            }
        else:
            resultado["promedio_clues"] = {"valor": None, "cols": None, "chart": None}

        # ── KPI 11: Top municipios por volumen — hbar top-10 ──
        chart11 = None
        top_mun_valor = None
        if c_mun and cols_dosis:
            tmp = df[[c_mun] + cols_dosis].copy()
            for c in cols_dosis:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
            ranking = (tmp.groupby(c_mun)[cols_dosis]
                         .sum().sum(axis=1)
                         .nlargest(10).sort_values())
            if not ranking.empty:
                top_mun_valor = int(ranking.iloc[-1])
                labels = [str(k)[:16] for k in ranking.index]
                chart11 = {"type": "hbar", "labels": labels,
                           "values": [int(v) for v in ranking.values]}
        resultado["top_municipios"] = {
            "valor": top_mun_valor,
            "cols": c_mun,
            "extra": "Top 10 municipios",
            "chart": chart11,
        }

        # ── KPI 12: Crecimiento mensual de dosis ──
        crecimiento = None
        chart12 = None
        if serie_total and len(serie_total) >= 2:
            meses_ord = sorted(serie_total.keys())
            ultimo  = serie_total[meses_ord[-1]]
            penult  = serie_total[meses_ord[-2]]
            if penult > 0:
                crecimiento = round((ultimo - penult) / penult * 100, 1)
            valores_meses = [serie_total.get(m, 0) for m in range(1, 13)]
            chart12 = {
                "type": "bar",
                "labels": NOMBRES_MES,
                "values": valores_meses,
            }
        resultado["crecimiento_mensual"] = {
            "valor": crecimiento,
            "cols": c_mes,
            "extra": "último mes vs. anterior" if crecimiento is not None else "",
            "chart": chart12,
        }

        # ── KPI 13: Cobertura municipal activa ──
        cobertura_pct = None
        chart13 = None
        if c_mun and cols_dosis:
            df_num = df[cols_dosis].apply(pd.to_numeric, errors="coerce")
            muns_activos = int(df.loc[df_num.sum(axis=1) > 0, c_mun].nunique())
            total_muns   = int(df[c_mun].nunique())
            if total_muns > 0:
                cobertura_pct = round(muns_activos / total_muns * 100, 1)
            chart13 = {"type": "donut", "value": muns_activos, "total": total_muns,
                       "label": f"/ {total_muns} mun. en datos"}
        resultado["cobertura_municipal"] = {
            "valor": cobertura_pct,
            "cols": c_mun,
            "extra": f"{muns_activos} de {total_muns} municipios activos" if cobertura_pct is not None else "",
            "chart": chart13,
        }

        # ── KPI 14: Vacuna más demandada ──
        vacuna_top_valor = None
        chart14 = None
        GRUPOS_VAC = {
            "BCG": cols_prefijo("VBC"),
            "Hepatitis B": cols_prefijo("VHB"),
            "VPH": cols_prefijo("VPH"),
            "Rotavirus": cols_prefijo("VRV"),
        }
        totales_vac = {}
        for nombre_vac, cols_vac in GRUPOS_VAC.items():
            t = suma_cols(cols_vac)
            if t:
                totales_vac[nombre_vac] = t
        mejor_vac = max(totales_vac, key=totales_vac.get) if totales_vac else None
        if mejor_vac:
            vacuna_top_valor = totales_vac[mejor_vac]
            items_ord = sorted(totales_vac.items(), key=lambda x: x[1])
            chart14 = {"type": "hbar",
                       "labels": [k for k, _ in items_ord],
                       "values": [v for _, v in items_ord]}
        resultado["vacuna_top"] = {
            "valor": vacuna_top_valor,
            "cols": mejor_vac,
            "extra": f"Líder: {mejor_vac}" if mejor_vac else "",
            "chart": chart14,
        }

        return resultado

    def _crear_card_kpi(self, parent, defn, valor, col_usada, extra, fila, col_grid):
        COLOR_GRIS = "#B0B8C9"
        chart_data = defn.get("chart_data")

        # ── Determinar color y texto de estado ──
        if valor is None:
            accent     = COLOR_GRIS
            accent_dim = "#D8DCE5"
            status_txt = "Sin datos"
            valor_str  = "N/A"
        else:
            accent     = COLOR_GRIS
            accent_dim = "#D8DCE5"
            status_txt = ""
            v = valor
            if isinstance(v, float) and v == int(v):
                v = int(v)
            if isinstance(v, int) and v >= 1_000_000:
                valor_str = f"{v / 1_000_000:.1f}M{defn['unidad']}"
            elif isinstance(v, (int, float)) and v >= 1_000:
                valor_str = f"{int(v):,}{defn['unidad']}"
            else:
                valor_str = f"{v}{defn['unidad']}"
            for lo, hi, color, texto in defn["umbrales"]:
                if valor >= lo and (hi is None or valor < hi):
                    accent     = color
                    accent_dim = color + "33"
                    status_txt = texto
                    break

        # ── Card frame ──
        card = tk.Frame(parent, bg=BLANCO, relief="flat",
                        highlightbackground=GRIS_BORDE, highlightthickness=1)
        card.grid(row=fila, column=col_grid, padx=8, pady=8, sticky="nsew")

        tk.Frame(card, bg=accent, height=5).pack(fill="x")

        content = tk.Frame(card, bg=BLANCO)
        content.pack(fill="both", expand=True, padx=14, pady=(8, 10))

        # ── Cabecera: número + título + valor ──
        top_row = tk.Frame(content, bg=BLANCO)
        top_row.pack(fill="x")

        left = tk.Frame(top_row, bg=BLANCO)
        left.pack(side="left", fill="y")

        tk.Label(left, text=f"KPI {defn['n']}", font=("Segoe UI", 7, "bold"),
                 bg=BLANCO, fg="#B0B8C9").pack(anchor="w")
        tk.Label(left, text=defn["titulo"], font=("Segoe UI", 10, "bold"),
                 bg=BLANCO, fg=TEXTO_OSCURO, wraplength=190,
                 justify="left").pack(anchor="w")
        if status_txt:
            badge = tk.Frame(left, bg=accent)
            tk.Label(badge, text=f"  {status_txt}  ",
                     font=("Segoe UI", 7, "bold"),
                     bg=accent, fg=BLANCO).pack(padx=1, pady=1)
            badge.pack(anchor="w", pady=(3, 0))

        right = tk.Frame(top_row, bg=BLANCO)
        right.pack(side="right", anchor="n")
        tk.Label(right, text=valor_str, font=("Segoe UI", 20, "bold"),
                 bg=BLANCO, fg=accent).pack(anchor="e")

        # ── Separador ──
        tk.Frame(content, bg=GRIS_BORDE, height=1).pack(fill="x", pady=(6, 0))

        # ── Gráfico ──
        self._dibujar_chart(content, chart_data, accent)

        # ── Pie de card ──
        foot = tk.Frame(content, bg=BLANCO)
        foot.pack(fill="x", pady=(4, 0))
        hint = col_usada if col_usada else defn.get("hint", "")
        if hint:
            tk.Label(foot, text=hint, font=("Consolas", 7),
                     bg=BLANCO, fg="#C0C8D5", wraplength=300,
                     justify="left").pack(side="left")
        if extra:
            tk.Label(foot, text=extra, font=("Segoe UI", 7),
                     bg=BLANCO, fg="#7A8499", wraplength=140,
                     justify="right").pack(side="right")

    def _dibujar_chart(self, parent, chart_data, accent):
        if not MATPLOTLIB_OK or not chart_data:
            return

        tipo = chart_data.get("type")

        try:
            if tipo == "bar":
                self._chart_bar(parent, chart_data, accent)
            elif tipo == "donut":
                self._chart_donut(parent, chart_data, accent)
            elif tipo == "hbar":
                self._chart_hbar(parent, chart_data, accent)
        except Exception:
            pass  # nunca romper la UI por un gráfico

    def _fig_base(self, parent, w, h):
        """Crea figura + canvas embebido. Devuelve (fig, ax)."""
        fig = Figure(figsize=(w, h), dpi=82, facecolor=BLANCO)
        ax  = fig.add_subplot(111, facecolor=BLANCO)
        if not hasattr(self, '_dash_figures'):
            self._dash_figures = []
        self._dash_figures.append(fig)
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.get_tk_widget().pack(fill="x", pady=(6, 0))
        return fig, ax, canvas

    def _chart_bar(self, parent, data, accent):
        labels = data["labels"]
        values = data["values"]
        if not any(values):
            return
        fig, ax, canvas = self._fig_base(parent, 4.2, 1.5)

        x = range(len(labels))
        bars = ax.bar(x, values, color=accent, alpha=0.85, width=0.65,
                      edgecolor="none", zorder=3)
        # Resaltar barra máxima
        max_v = max(values)
        for bar, v in zip(bars, values):
            if v == max_v:
                bar.set_alpha(1.0)
                bar.set_edgecolor(accent)
                bar.set_linewidth(1.2)

        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, fontsize=6.5, color="#7A8499")
        ax.set_yticks([])
        ax.spines["left"].set_visible(False)
        ax.spines["bottom"].set_color("#E8ECF3")
        ax.tick_params(axis="x", length=0)
        ax.grid(axis="y", color="#F0F0F0", zorder=0)
        fig.tight_layout(pad=0.3)
        canvas.draw()

    def _chart_donut(self, parent, data, accent):
        value = data.get("value") or 0
        total = data.get("total") or 1
        label = data.get("label", "")
        fig, ax, canvas = self._fig_base(parent, 4.2, 1.7)

        resto  = max(total - value, 0)
        colors = [accent, "#EEF0F5"]
        wedges, _ = ax.pie(
            [value, resto], colors=colors,
            startangle=90, counterclock=False,
            wedgeprops=dict(width=0.45, edgecolor=BLANCO, linewidth=2)
        )
        pct = round(value / total * 100) if total else 0
        ax.text(0, 0.08, f"{value}", ha="center", va="center",
                fontsize=13, fontweight="bold", color=accent)
        ax.text(0, -0.28, label, ha="center", va="center",
                fontsize=6.5, color="#7A8499")
        ax.text(0, -0.62, f"{pct}%", ha="center", va="center",
                fontsize=8, fontweight="bold", color=accent)
        fig.tight_layout(pad=0.2)
        canvas.draw()

    def _chart_hbar(self, parent, data, accent):
        labels = data["labels"]
        values = data["values"]
        if not any(values):
            return
        n = len(labels)
        fig, ax, canvas = self._fig_base(parent, 4.2, 0.38 * n + 0.4)

        y      = range(n)
        max_v  = max(values) or 1
        colors = [accent if v == max_v else accent + "99" for v in values]
        ax.barh(list(y), values, color=colors, edgecolor="none", height=0.55)
        ax.set_yticks(list(y))
        ax.set_yticklabels(labels, fontsize=6.5, color="#7A8499")
        ax.set_xticks([])
        ax.spines["bottom"].set_visible(False)
        ax.spines["left"].set_color("#E8ECF3")
        ax.tick_params(axis="y", length=0)
        # Etiqueta de valor al final de cada barra
        for i, v in enumerate(values):
            s = f"{v/1_000_000:.1f}M" if v >= 1_000_000 else f"{v:,}"
            ax.text(v + max_v * 0.02, i, s, va="center",
                    fontsize=6, color="#7A8499")
        ax.set_xlim(0, max_v * 1.25)
        fig.tight_layout(pad=0.3)
        canvas.draw()

    def _mostrar_placeholder(self):
        self.tabla.delete(*self.tabla.get_children())
        self.tabla["columns"] = ("msg",)
        self.tabla["show"] = "headings"
        self.tabla.heading("msg", text="")
        self.tabla.column("msg", width=600, anchor="center")
        self.tabla.insert("", "end", values=("Selecciona un archivo y presiona EXTRAER DATOS para comenzar",))

    # ══════════════════════════════════════════
    #  LÓGICA DE NEGOCIO
    # ══════════════════════════════════════════

    def explorar_archivo(self):
        ruta = filedialog.askopenfilename(
            title="Seleccionar archivo Excel o CSV",
            filetypes=[
                ("Todos los soportados", "*.xlsx *.xls *.xlsm *.csv"),
                ("Excel files", "*.xlsx *.xls *.xlsm"),
                ("CSV files", "*.csv"),
                ("Todos los archivos", "*.*")
            ],
            initialdir=os.path.expanduser("~/Documents")
        )
        if ruta:
            self.archivo_cargado = ruta
            nombre = os.path.basename(ruta)
            self.lbl_archivo.config(text=nombre, fg=VERDE_SALUD)
            self._set_status(f"Archivo seleccionado: {nombre}")
            self.lbl_resumen_extraccion.config(text="")

            es_csv = ruta.lower().endswith(".csv")
            if es_csv:
                self.combo_hoja["values"] = ["(CSV -- hoja unica)"]
                self.combo_hoja.current(0)
                self.combo_hoja.config(state="disabled")
            else:
                self.combo_hoja.config(state="readonly")
                try:
                    xl = pd.ExcelFile(ruta)
                    self.combo_hoja["values"] = xl.sheet_names
                    self.combo_hoja.current(0)
                except Exception as e:
                    messagebox.showerror("Error", f"No se pudo leer el archivo:\n{e}")

    def descargar_desde_url(self):
        url = self.entry_url.get().strip()
        if not url or url.startswith("https://raw.github") and len(url) < 50:
            messagebox.showwarning("URL vacía", "Por favor ingresa una URL válida.")
            return

        self.lbl_url_estado.config(text="Descargando...", fg="#F59E0B")
        self.update_idletasks()

        def _worker():
            try:
                import requests, tempfile, urllib.parse
                headers = {"User-Agent": "Mozilla/5.0"}
                resp = requests.get(url, headers=headers, timeout=60, stream=True)
                resp.raise_for_status()

                # Determinar extensión
                parsed = urllib.parse.urlparse(url)
                ext = os.path.splitext(parsed.path)[1].lower() or ".csv"
                if ext not in (".csv", ".xlsx", ".xls", ".xlsm"):
                    ext = ".csv"

                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                    for chunk in resp.iter_content(chunk_size=65536):
                        tmp.write(chunk)
                    tmp_path = tmp.name

                def _on_ok():
                    nombre = os.path.basename(parsed.path) or "archivo_url" + ext

                    # Validar columnas mínimas del formato SIS
                    cols_minimas = {"ENTIDAD", "MUNICIPIO", "CLUES", "MES"}
                    faltantes = set()
                    try:
                        if ext == ".csv":
                            muestra = pd.read_csv(tmp_path, nrows=1)
                        else:
                            muestra = pd.read_excel(tmp_path, nrows=1)
                        cols_upper = {c.upper() for c in muestra.columns}
                        faltantes = cols_minimas - cols_upper
                        if faltantes:
                            self.lbl_url_estado.config(
                                text=f"Advertencia: el archivo no tiene el formato SIS esperado "
                                     f"(faltan: {', '.join(sorted(faltantes))}). "
                                     f"El archivo se cargó pero puede fallar al extraer.",
                                fg="#F59E0B"
                            )
                            messagebox.showwarning(
                                "Formato no compatible",
                                f"El archivo descargado no parece ser un export del SIS de la SSA.\n\n"
                                f"Columnas faltantes: {', '.join(sorted(faltantes))}\n\n"
                                f"Se necesitan: ENTIDAD, MUNICIPIO, CLUES, MES, y columnas de dosis "
                                f"(VBC01, VHB01, VPH01, VRV01...).\n\n"
                                f"Fuente correcta: exportes del Sistema de Información en Salud (SIS) "
                                f"de la Secretaría de Salud de México."
                            )
                    except Exception:
                        pass

                    self.archivo_cargado = tmp_path
                    self.lbl_archivo.config(text=f"[URL] {nombre}", fg=VERDE_SALUD)
                    if not faltantes:
                        self.lbl_url_estado.config(
                            text=f"Descargado correctamente ({os.path.getsize(tmp_path)//1024} KB)",
                            fg=VERDE_SALUD
                        )
                    if ext == ".csv":
                        self.combo_hoja["values"] = ["(CSV -- hoja unica)"]
                        self.combo_hoja.current(0)
                        self.combo_hoja.config(state="disabled")
                    else:
                        self.combo_hoja.config(state="readonly")
                        try:
                            xl = pd.ExcelFile(tmp_path)
                            self.combo_hoja["values"] = xl.sheet_names
                            self.combo_hoja.current(0)
                        except Exception:
                            pass
                    self._set_status(f"Archivo desde URL listo: {nombre}")
                    self.lbl_resumen_extraccion.config(text="")

                self.after(0, _on_ok)

            except Exception as e:
                err = str(e)
                def _on_err(msg=err):
                    self.lbl_url_estado.config(text=f"Error: {msg}", fg="#EF4444")
                    messagebox.showerror("Error al descargar", msg)
                self.after(0, _on_err)

        threading.Thread(target=_worker, daemon=True).start()

    def explorar_archivos_no_estructurados(self):
        rutas = filedialog.askopenfilenames(
            title="Seleccionar archivos no estructurados",
            filetypes=[
                ("Archivos soportados", "*.txt *.json *.pdf"),
                ("Texto", "*.txt"),
                ("JSON", "*.json"),
                ("PDF", "*.pdf"),
                ("Todos los archivos", "*.*")
            ],
            initialdir=os.path.expanduser("~/Documents")
        )
        if rutas:
            self.archivos_no_estructurados = list(rutas)
            nombres = [os.path.basename(r) for r in rutas]
            self.lbl_archivos_ne.config(
                text=f"{len(rutas)} archivo(s): {', '.join(nombres)}",
                fg=VERDE_SALUD
            )
            self._set_status(f"{len(rutas)} archivo(s) no estructurado(s) seleccionado(s)")

    def ejecutar_extraccion(self):
        if not self.archivo_cargado:
            messagebox.showwarning(
                "Archivo no seleccionado",
                "Por favor selecciona un archivo primero.\n\n"
                "Usa el boton Explorar archivo..."
            )
            return

        self.btn_extraer.config(state="disabled", text="Procesando...")
        self.progreso_extraer.start(10)
        tipo = "CSV" if self.archivo_cargado.lower().endswith(".csv") else "Excel"
        self._set_status(f"Leyendo datos del archivo {tipo}...")

        hilo = threading.Thread(target=self._cargar_datos_thread, daemon=True)
        hilo.start()

    def _cargar_datos_thread(self):
        try:
            time.sleep(0.3)
            es_csv = self.archivo_cargado.lower().endswith(".csv")

            if es_csv:
                self.root.after(0, lambda: self._set_status("Leyendo archivo CSV..."))
                time.sleep(0.2)

                encodings = ["utf-8", "utf-8-sig", "latin-1", "iso-8859-1", "cp1252"]
                df = None
                for enc in encodings:
                    try:
                        df = pd.read_csv(self.archivo_cargado, encoding=enc, low_memory=False)
                        break
                    except (UnicodeDecodeError, Exception):
                        continue

                if df is None:
                    raise Exception("No se pudo detectar el encoding del CSV.")

                self.root.after(0, lambda: self._set_status("Optimizando datos..."))
                time.sleep(0.3)
                df = pd.DataFrame(df)
                df["_hoja"] = "CSV"
                self.df = df

            else:
                self.root.after(0, lambda: self._set_status("Leyendo todas las hojas..."))
                time.sleep(0.2)

                todas_hojas = pd.read_excel(self.archivo_cargado, sheet_name=None)

                self.root.after(0, lambda: self._set_status("Consolidando datos..."))
                time.sleep(0.2)

                frames = []
                for nombre_hoja, df_hoja in todas_hojas.items():
                    df_hoja = df_hoja.copy()
                    df_hoja["_hoja"] = nombre_hoja
                    frames.append(df_hoja)

                df_temp = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                self.df = df_temp.copy() if not df_temp.empty else df_temp

            self.root.after(0, self._extraccion_completada)

        except Exception as e:
            self.root.after(0, lambda: self._error_carga(str(e)))

    def _extraccion_completada(self):
        self.progreso_extraer.stop()
        self.btn_extraer.config(state="normal", text="EXTRAER DATOS")

        if self.df is None or self.df.empty:
            messagebox.showinfo("Sin datos", "El archivo no contiene datos.")
            return

        total = len(self.df)
        cols = len(self.df.columns)
        es_csv = self.archivo_cargado.lower().endswith(".csv")

        # Detectar tipos de columnas
        num_cols = len([c for c in self.df.columns if c != "_hoja" and self.df[c].dtype in ['int64', 'float64']])
        text_cols = len([c for c in self.df.columns if c != "_hoja" and self.df[c].dtype == 'object'])

        resumen = f"Registros extraidos: {total:,}\n"
        resumen += f"Columnas detectadas: {cols}\n"
        resumen += f"Columnas numericas: {num_cols}\n"
        resumen += f"Columnas de texto: {text_cols}"
        if not es_csv:
            hojas = self.df['_hoja'].nunique()
            resumen += f"\nHojas procesadas: {hojas}"

        self.lbl_resumen_extraccion.config(text=resumen, fg=VERDE_SALUD)
        self._set_status(f"Extraccion completada -- {total:,} registros, {cols} columnas")

        self._defragmentar_dataframe()

        if self.mongo:
            try:
                archivo_nombre = os.path.basename(self.archivo_cargado)
                self.mongo.registrar_extraccion(
                    archivo=archivo_nombre,
                    estadisticas={
                        'filas': len(self.df),
                        'columnas': len(self.df.columns),
                        'estado_resumen': self.df['estado'].value_counts().to_dict()
                                        if 'estado' in self.df.columns else {}
                    }
                )
                print(f"✓ MongoDB: Extraccion registrada")
                self._set_status(f"Extraccion registrada en MongoDB")

            except Exception as e:
                print(f"✗ Error al registrar en MongoDB: {e}")
                self._set_status(f"⚠ Error MongoDB: {str(e)}")

        # Navegar automáticamente a pantalla de transformación
        self.root.after(800, self._iniciar_transformacion)

    def _error_carga(self, msg):
        self.progreso_extraer.stop()
        self.btn_extraer.config(state="normal", text="EXTRAER DATOS")
        self._set_status("Error al cargar datos")
        messagebox.showerror("Error de carga", f"No se pudieron cargar los datos:\n\n{msg}")

    # ══════════════════════════════════════════
    #  TRANSFORMACIÓN DE DATOS
    # ══════════════════════════════════════════

    def _clasificar_columnas(self, df):
        estructuradas = []
        no_estructuradas = []
        for col in df.columns:
            if col == "_hoja":
                continue
            if df[col].dtype in ['int64', 'float64']:
                estructuradas.append(col)
            elif df[col].dtype == 'object':
                avg_len = df[col].dropna().astype(str).str.len().mean()
                if avg_len > 30:
                    no_estructuradas.append(col)
                else:
                    estructuradas.append(col)
            else:
                estructuradas.append(col)
        return estructuradas, no_estructuradas

    def _defragmentar_dataframe(self):
        """Defragmenta el DataFrame completamente para archivos grandes."""
        if self.df is None or self.df.empty:
            return
        self.df = pd.DataFrame(self.df)

    def _homogeneizar_datos(self):
        """Normaliza abreviaturas de estados mexicanos y limpia texto."""
        resumen = {}
        if self.df is None:
            return resumen

        df = self.df.copy()

        for col in df.columns:
            if col == "_hoja":
                continue
            if df[col].dtype != "object":
                continue

            # Limpieza básica: strip y colapsar espacios múltiples
            antes = df[col].copy()
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
            df[col] = df[col].replace("nan", pd.NA)

            espacios_limpiados = (antes.astype(str) != df[col].astype(str)).sum()

            # Detectar si es columna de estados: >30% de valores únicos coinciden
            valores_unicos = df[col].dropna().str.lower().str.strip().unique()
            if len(valores_unicos) == 0:
                continue

            coincidencias = sum(1 for v in valores_unicos if v in ESTADOS_MEXICO)
            ratio = coincidencias / len(valores_unicos)

            estados_normalizados = 0
            if ratio > 0.3:
                # Aplicar normalización de estados
                def normalizar_estado(val):
                    if pd.isna(val):
                        return val
                    key = str(val).lower().strip()
                    return ESTADOS_MEXICO.get(key, val)

                antes_norm = df[col].copy()
                df[col] = df[col].apply(normalizar_estado)
                estados_normalizados = (antes_norm != df[col]).sum()

            if espacios_limpiados > 0 or estados_normalizados > 0:
                resumen[col] = {
                    "espacios_limpiados": int(espacios_limpiados),
                    "estados_normalizados": int(estados_normalizados)
                }

        self.df = df
        self.resumen_homogeneizacion = resumen
        return resumen

    def _cargar_archivos_no_estructurados(self):
        """Lee contenido de archivos TXT, JSON y PDF seleccionados."""
        self.contenido_archivos_externos = {}

        for ruta in self.archivos_no_estructurados:
            nombre = os.path.basename(ruta)
            ext = os.path.splitext(ruta)[1].lower()
            try:
                if ext == ".txt":
                    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
                    contenido = None
                    for enc in encodings:
                        try:
                            with open(ruta, "r", encoding=enc) as f:
                                contenido = f.read()
                            break
                        except (UnicodeDecodeError, Exception):
                            continue
                    if contenido is None:
                        contenido = "[Error: no se pudo leer el archivo con los encodings disponibles]"
                    self.contenido_archivos_externos[nombre] = contenido

                elif ext == ".json":
                    import json
                    with open(ruta, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    self.contenido_archivos_externos[nombre] = json.dumps(data, indent=2, ensure_ascii=False)

                elif ext == ".pdf":
                    try:
                        from PyPDF2 import PdfReader
                        reader = PdfReader(ruta)
                        texto = ""
                        for page in reader.pages:
                            texto += page.extract_text() or ""
                            texto += "\n"
                        self.contenido_archivos_externos[nombre] = texto if texto.strip() else "[PDF sin texto extraible]"
                    except ImportError:
                        self.contenido_archivos_externos[nombre] = (
                            "[PyPDF2 no esta instalado. Ejecuta: pip install PyPDF2]"
                        )
                else:
                    self.contenido_archivos_externos[nombre] = "[Formato no soportado]"

            except Exception as e:
                self.contenido_archivos_externos[nombre] = f"[Error al leer: {e}]"

    def _iniciar_transformacion(self):
        self.mostrar_pantalla("transformar")
        self.progreso_transformar["value"] = 0
        self.lbl_paso_transformar.config(text="")
        self._activar_chip(0)

        # Iniciar simulación de transformación en hilo
        hilo = threading.Thread(target=self._proceso_transformacion, daemon=True)
        hilo.start()

    def _proceso_transformacion(self):
        pasos = [
            (10, "Clasificando columnas por tipo de dato..."),
            (20, "Detectando texto libre y comentarios..."),
            (35, "Homogeneizando datos (estados, espacios)..."),
            (50, "Cargando archivos no estructurados..."),
            (60, "Limpiando valores nulos..."),
            (75, "Guardando datos internos en MongoDB..."),
            (85, "Guardando datos no estructurados en MongoDB..."),
            (92, "Guardando datos externos en MongoDB..."),
            (98, "Generando resumen de transformacion..."),
            (100, "Transformacion completada"),
        ]

        for idx, (valor, texto) in enumerate(pasos):
            self.root.after(0, lambda v=valor, t=texto: self._actualizar_progreso_transformar(v, t))
            time.sleep(0.4)

            if idx == 2:
                self._homogeneizar_datos()
            elif idx == 3:
                self._cargar_archivos_no_estructurados()

        # Clasificar columnas
        self.cols_estructuradas, self.cols_no_estructuradas = self._clasificar_columnas(self.df)

        # Guardar en las 3 colecciones de MongoDB
        res_mongo = self._guardar_en_mongodb()

        # Contar nulos limpiados
        nulos_total = self.df.isnull().sum().sum()

        resultado = f"RESULTADO DE LA TRANSFORMACION\n\n"
        resultado += f"Columnas estructuradas: {len(self.cols_estructuradas)}\n"
        for c in self.cols_estructuradas:
            resultado += f"   - {c} ({self.df[c].dtype})\n"
        resultado += f"\nColumnas no estructuradas (texto libre): {len(self.cols_no_estructuradas)}\n"
        for c in self.cols_no_estructuradas:
            avg = self.df[c].dropna().astype(str).str.len().mean()
            resultado += f"   - {c} (promedio {avg:.0f} caracteres)\n"
        resultado += f"\nValores nulos detectados: {nulos_total:,}\n"
        resultado += f"Total de registros procesados: {len(self.df):,}"

        # Resumen de homogeneización
        if self.resumen_homogeneizacion:
            resultado += f"\n\nHOMOGENEIZACION DE DATOS:\n"
            for col, info in self.resumen_homogeneizacion.items():
                detalles = []
                if info["espacios_limpiados"] > 0:
                    detalles.append(f"{info['espacios_limpiados']} espacios limpiados")
                if info["estados_normalizados"] > 0:
                    detalles.append(f"{info['estados_normalizados']} estados normalizados")
                resultado += f"   - {col}: {', '.join(detalles)}\n"

        # Resumen de archivos externos
        if self.contenido_archivos_externos:
            resultado += f"\nARCHIVOS EXTERNOS CARGADOS: {len(self.contenido_archivos_externos)}\n"
            for nombre in self.contenido_archivos_externos:
                resultado += f"   - {nombre}\n"

        # Resumen de MongoDB
        if res_mongo:
            resultado += f"\nMONGODB (3 colecciones):\n"
            for col_nombre, info in res_mongo.items():
                if info.get('exito'):
                    resultado += f"   - {col_nombre}: {info['registros_insertados']} registros\n"
                else:
                    resultado += f"   - {col_nombre}: Error - {info.get('error', 'desconocido')}\n"

        self.root.after(0, lambda: self._transformacion_completada(resultado))

    def _guardar_en_mongodb(self):
        """Guarda datos en las 3 colecciones de MongoDB"""
        if not self.mongo:
            return None

        resultados = {}
        archivo_nombre = os.path.basename(self.archivo_cargado) if self.archivo_cargado else "desconocido"

        try:
            # 1. Datos internos (estructurados)
            res = self.mongo.guardar_datos_internos(self.df, self.cols_estructuradas, archivo_nombre)
            resultados['datos_internos'] = res
            print(f"  MongoDB datos_internos: {res.get('registros_insertados', 0)} registros")
        except Exception as e:
            resultados['datos_internos'] = {'exito': False, 'error': str(e)}
            print(f"  MongoDB datos_internos error: {e}")

        try:
            # 2. Datos no estructurados (texto libre del Excel)
            if self.cols_no_estructuradas:
                res = self.mongo.guardar_datos_no_estructurados(self.df, self.cols_no_estructuradas, archivo_nombre)
            else:
                res = {'exito': True, 'registros_insertados': 0}
            resultados['datos_no_estructurados'] = res
            print(f"  MongoDB datos_no_estructurados: {res.get('registros_insertados', 0)} registros")
        except Exception as e:
            resultados['datos_no_estructurados'] = {'exito': False, 'error': str(e)}
            print(f"  MongoDB datos_no_estructurados error: {e}")

        try:
            # 3. Datos externos (archivos TXT/JSON/PDF)
            if self.contenido_archivos_externos:
                res = self.mongo.guardar_datos_externos(self.contenido_archivos_externos, archivo_nombre)
            else:
                res = {'exito': True, 'registros_insertados': 0}
            resultados['datos_externos'] = res
            print(f"  MongoDB datos_externos: {res.get('registros_insertados', 0)} registros")
        except Exception as e:
            resultados['datos_externos'] = {'exito': False, 'error': str(e)}
            print(f"  MongoDB datos_externos error: {e}")

        return resultados

    def _actualizar_progreso_transformar(self, valor, texto):
        self.progreso_transformar["value"] = valor
        self.lbl_paso_transformar.config(text=texto)
        chip_idx = min(int(valor / 100 * len(self._chips)), len(self._chips) - 1)
        self._activar_chip(chip_idx)

    def _transformacion_completada(self, resultado):
        self._activar_chip(len(self._chips))  # todos verdes
        self._set_status("✓  Datos listos")
        self._preparar_pantalla_mostrar()
        self.actualizar_dashboard()
        self.root.after(900, lambda: self.mostrar_pantalla("mostrar"))

    def _preparar_pantalla_mostrar(self):
        # Poblar tabla con datos estructurados (+ columnas no estructuradas en tabla también)
        self._poblar_tabla(self.df)
        self._actualizar_estadisticas(self.df)
        self._poblar_texto_no_estructurado()

    # ══════════════════════════════════════════
    #  FUNCIONES DE LA PANTALLA MOSTRAR
    # ══════════════════════════════════════════

    def _poblar_tabla(self, df):
        self.tabla.delete(*self.tabla.get_children())

        # Mostrar todas las columnas (estructuradas + _hoja)
        cols_mostrar = [c for c in df.columns if c != "_hoja"]
        if "_hoja" in df.columns:
            cols_mostrar.append("_hoja")

        self.tabla["columns"] = cols_mostrar
        self.tabla["show"] = "headings"

        for col in cols_mostrar:
            ancho = max(80, min(200, len(str(col)) * 10))
            self.tabla.heading(col, text=col)
            self.tabla.column(col, width=ancho, minwidth=60)

        for i, (_, fila) in enumerate(df.head(5000).iterrows()):
            valores = [str(v) if pd.notna(v) else "" for v in fila[cols_mostrar]]
            tag = "par" if i % 2 == 0 else "impar"
            self.tabla.insert("", "end", values=valores, tags=(tag,))

        self.tabla.tag_configure("par", background="#F8FAFC")
        self.tabla.tag_configure("impar", background=BLANCO)

        mostrados = min(len(df), 5000)
        self.lbl_conteo.config(
            text=f"Mostrando {mostrados:,} de {len(df):,} registros"
        )
        self.lbl_titulo_tabla.config(
            text=f"Datos Estructurados -- {os.path.basename(self.archivo_cargado)}"
        )

    def _poblar_texto_no_estructurado(self):
        self.txt_no_estructurado.config(state="normal")
        self.txt_no_estructurado.delete("1.0", "end")

        if not self.cols_no_estructuradas:
            self.txt_no_estructurado.insert("end",
                "No se detectaron columnas de texto no estructurado en este archivo.\n\n"
                "Las columnas de texto no estructurado son aquellas con un promedio\n"
                "de longitud mayor a 30 caracteres (comentarios, observaciones, etc.)")
            self.lbl_conteo_no_estruct.config(text="0 columnas de texto libre")
        else:
            self.lbl_conteo_no_estruct.config(
                text=f"{len(self.cols_no_estructuradas)} columna(s) de texto libre"
            )
            # Mostrar los primeros 200 registros con texto no estructurado
            df_muestra = self.df.head(200)
            for i, (_, fila) in enumerate(df_muestra.iterrows()):
                self.txt_no_estructurado.insert("end", f"--- Registro {i + 1} ---\n")
                for col in self.cols_no_estructuradas:
                    valor = fila[col]
                    if pd.notna(valor) and str(valor).strip():
                        self.txt_no_estructurado.insert("end",
                            f"  [{col}]: {valor}\n")
                self.txt_no_estructurado.insert("end", "\n")

        # Mostrar contenido de archivos externos cargados
        if self.contenido_archivos_externos:
            self.txt_no_estructurado.insert("end",
                "\n" + "=" * 60 + "\n"
                "  ARCHIVOS EXTERNOS NO ESTRUCTURADOS\n"
                "=" * 60 + "\n\n")
            for nombre, contenido in self.contenido_archivos_externos.items():
                self.txt_no_estructurado.insert("end", f"--- {nombre} ---\n")
                # Limitar preview a 5000 caracteres por archivo
                if len(contenido) > 5000:
                    self.txt_no_estructurado.insert("end", contenido[:5000])
                    self.txt_no_estructurado.insert("end",
                        f"\n\n[... truncado, {len(contenido):,} caracteres totales ...]\n\n")
                else:
                    self.txt_no_estructurado.insert("end", contenido + "\n\n")

        self.txt_no_estructurado.config(state="disabled")

    def _actualizar_estadisticas(self, df):
        total = len(df)
        cols = len(df.columns)
        nulos = df.isnull().sum().sum()
        hojas = df["_hoja"].nunique() if "_hoja" in df.columns else 1
        tam = df.memory_usage(deep=True).sum() / 1024

        texto = (
            f"Registros: {total:,}  |  "
            f"Columnas: {cols}  |  "
            f"Hojas: {hojas}  |  "
            f"Valores nulos: {nulos:,}  |  "
            f"Estructuradas: {len(self.cols_estructuradas)}  |  "
            f"No estructuradas: {len(self.cols_no_estructuradas)}  |  "
            f"Tam: {tam:.1f} KB"
        )
        self.lbl_stats.config(text=texto, fg=TEXTO_OSCURO)

    def filtrar_datos(self, event=None):
        if self.df is None:
            return
        texto = self.entry_buscar.get().lower().strip()
        if not texto:
            self._poblar_tabla(self.df)
            return
        try:
            mascara = self.df.apply(
                lambda col: col.astype(str).str.lower().str.contains(texto, na=False)
            ).any(axis=1)
            df_filtrado = self.df[mascara]
            self._poblar_tabla(df_filtrado)
            self._set_status(f"Filtro aplicado -- {len(df_filtrado):,} coincidencias")
        except Exception:
            pass

    def limpiar_filtros(self):
        self.entry_buscar.delete(0, "end")
        if self.df is not None:
            self._poblar_tabla(self.df)
            self._set_status("Filtros limpiados")

    def exportar_csv(self):
        if self.df is None:
            messagebox.showwarning("Sin datos", "Primero extrae un archivo.")
            return
        ruta = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile="datos_vacunacion_oaxaca.csv"
        )
        if ruta:
            self.df.to_csv(ruta, index=False, encoding="utf-8-sig")
            self._set_status(f"CSV exportado: {os.path.basename(ruta)}")
            messagebox.showinfo("Exportado", f"Archivo guardado:\n{ruta}")

    def generar_reporte(self):
        if self.df is None:
            messagebox.showwarning("Sin datos", "Primero extrae un archivo.")
            return
        ruta = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Texto", "*.txt")],
            initialfile="reporte_dss_vacunacion.txt"
        )
        if not ruta:
            return

        with open(ruta, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("  DSS-VacOaxaca - Reporte de Recopilacion de Datos\n")
            f.write(f"  Generado: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write("=" * 60 + "\n\n")

            f.write(f"Archivo fuente: {self.archivo_cargado}\n")
            f.write(f"Total de registros: {len(self.df):,}\n")
            f.write(f"Total de columnas: {len(self.df.columns)}\n\n")

            f.write("COLUMNAS ESTRUCTURADAS:\n")
            for col in self.cols_estructuradas:
                dtype = str(self.df[col].dtype)
                nulos = self.df[col].isnull().sum()
                f.write(f"  - {col:<30} Tipo: {dtype:<12} Nulos: {nulos}\n")

            f.write(f"\nCOLUMNAS NO ESTRUCTURADAS (TEXTO LIBRE):\n")
            for col in self.cols_no_estructuradas:
                dtype = str(self.df[col].dtype)
                nulos = self.df[col].isnull().sum()
                avg_len = self.df[col].dropna().astype(str).str.len().mean()
                f.write(f"  - {col:<30} Tipo: {dtype:<12} Nulos: {nulos}  Longitud promedio: {avg_len:.0f}\n")

            # Sección de homogeneización
            if self.resumen_homogeneizacion:
                f.write(f"\nHOMOGENEIZACION DE DATOS:\n")
                for col, info in self.resumen_homogeneizacion.items():
                    detalles = []
                    if info["espacios_limpiados"] > 0:
                        detalles.append(f"{info['espacios_limpiados']} espacios limpiados")
                    if info["estados_normalizados"] > 0:
                        detalles.append(f"{info['estados_normalizados']} estados normalizados")
                    f.write(f"  - {col}: {', '.join(detalles)}\n")
                f.write("\n")

            f.write("\nESTADISTICAS NUMERICAS:\n")
            try:
                desc = self.df.describe(include="all").to_string()
                f.write(desc + "\n")
            except Exception:
                f.write("  (No disponibles)\n")

            if self.cols_no_estructuradas:
                f.write(f"\nMUESTRA DE DATOS NO ESTRUCTURADOS (primeros 20 registros):\n")
                f.write("-" * 60 + "\n")
                for i, (_, fila) in enumerate(self.df.head(20).iterrows()):
                    f.write(f"\n--- Registro {i + 1} ---\n")
                    for col in self.cols_no_estructuradas:
                        valor = fila[col]
                        if pd.notna(valor) and str(valor).strip():
                            f.write(f"  [{col}]: {valor}\n")

            # Sección de archivos externos
            if self.contenido_archivos_externos:
                f.write(f"\n\nARCHIVOS EXTERNOS NO ESTRUCTURADOS:\n")
                f.write("=" * 60 + "\n")
                for nombre, contenido in self.contenido_archivos_externos.items():
                    f.write(f"\n--- {nombre} ---\n")
                    if len(contenido) > 5000:
                        f.write(contenido[:5000])
                        f.write(f"\n[... truncado, {len(contenido):,} caracteres totales ...]\n")
                    else:
                        f.write(contenido + "\n")

        self._set_status(f"Reporte generado: {os.path.basename(ruta)}")
        messagebox.showinfo("Reporte generado", f"Reporte guardado en:\n{ruta}")

    # ══════════════════════════════════════════
    #  UTILIDADES
    # ══════════════════════════════════════════
    def _set_status(self, msg):
        self.lbl_status.config(text=f"  {msg}")

    def _hover(self, widget, color_normal, color_hover):
        widget.bind("<Enter>", lambda e: widget.config(bg=color_hover))
        widget.bind("<Leave>", lambda e: widget.config(bg=color_normal))

    def desconectar_mongodb(self):
        """Cierra la conexión a MongoDB al salir de la aplicación"""
        if hasattr(self, 'mongo') and self.mongo:
            try:
                self.mongo.desconectar()
                print("✓ Conexión a MongoDB cerrada correctamente")
            except Exception as e:
                print(f"✗ Error al cerrar conexión a MongoDB: {e}")


# ══════════════════════════════════════════════
#  PUNTO DE ENTRADA
# ══════════════════════════════════════════════
if __name__ == "__main__":
    root = ttk_boot.Window(themename="flatly")
    app = DSSVacunacionApp(root)

    def on_closing():
        if messagebox.askokcancel("Salir", "¿Deseas cerrar la aplicación?"):
            app.desconectar_mongodb()
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()
