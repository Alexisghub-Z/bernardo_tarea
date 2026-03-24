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
        if nombre == "extraer":
            self.btn_nav_extraer.config(bg=VERDE_SALUD)
            self.btn_nav_mostrar.config(bg=AZUL_GOB)
        elif nombre == "mostrar":
            self.btn_nav_extraer.config(bg=AZUL_GOB)
            self.btn_nav_mostrar.config(bg=VERDE_SALUD)
        else:
            self.btn_nav_extraer.config(bg=AZUL_GOB)
            self.btn_nav_mostrar.config(bg=AZUL_GOB)

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
                                                length=400,
                                                style="success.Horizontal.TProgressbar")
        self.progreso_extraer.pack(pady=(0, 15))

        # Resumen de extracción
        self.lbl_resumen_extraccion = tk.Label(
            center,
            text="",
            font=("Segoe UI", 10),
            bg=GRIS_FONDO, fg=TEXTO_OSCURO,
            justify="left"
        )
        self.lbl_resumen_extraccion.pack()

    # ══════════════════════════════════════════
    #  PANTALLA 2: TRANSFORMAR
    # ══════════════════════════════════════════
    def _construir_pantalla_transformar(self):
        frame = tk.Frame(self.contenedor, bg=GRIS_FONDO)
        self.pantallas["transformar"] = frame

        center = tk.Frame(frame, bg=GRIS_FONDO)
        center.place(relx=0.5, rely=0.45, anchor="center")

        tk.Label(
            center,
            text="TRANSFORMANDO DATOS",
            font=("Segoe UI", 22, "bold"),
            bg=GRIS_FONDO, fg=AZUL_GOB
        ).pack(pady=(0, 5))

        tk.Label(
            center,
            text="Clasificando y procesando la informacion extraida...",
            font=("Segoe UI", 11),
            bg=GRIS_FONDO, fg="#7A8499"
        ).pack(pady=(0, 30))

        # Barra de progreso de transformación
        self.progreso_transformar = ttk.Progressbar(center, mode="determinate",
                                                     length=500,
                                                     style="success.Horizontal.TProgressbar")
        self.progreso_transformar.pack(pady=(0, 20))

        # Etiqueta de paso actual
        self.lbl_paso_transformar = tk.Label(
            center,
            text="",
            font=("Segoe UI", 11),
            bg=GRIS_FONDO, fg=VERDE_SALUD
        )
        self.lbl_paso_transformar.pack(pady=(0, 25))

        # Card de resultados de transformación
        self.card_resultado_transform = tk.Frame(center, bg=BLANCO, relief="flat",
                                                  highlightbackground=GRIS_BORDE,
                                                  highlightthickness=1)
        self.card_resultado_transform.pack(fill="x", padx=20, pady=(0, 20))

        self.lbl_resultado_transform = tk.Label(
            self.card_resultado_transform,
            text="",
            font=("Segoe UI", 10),
            bg=BLANCO, fg=TEXTO_OSCURO,
            justify="left", anchor="w",
            wraplength=500
        )
        self.lbl_resultado_transform.pack(padx=25, pady=20)

        # Botón Ver Datos
        self.btn_ver_datos = tk.Button(
            center,
            text="VER DATOS",
            font=("Segoe UI", 13, "bold"),
            bg=VERDE_SALUD, fg=BLANCO,
            relief="flat", cursor="hand2",
            padx=35, pady=14,
            command=lambda: self.mostrar_pantalla("mostrar"),
            state="disabled"
        )
        self.btn_ver_datos.pack(pady=(5, 0))
        self._hover(self.btn_ver_datos, VERDE_SALUD, "#005C3D")

    # ══════════════════════════════════════════
    #  PANTALLA 3: MOSTRAR
    # ══════════════════════════════════════════
    def _construir_pantalla_mostrar(self):
        frame = tk.Frame(self.contenedor, bg=GRIS_FONDO)
        self.pantallas["mostrar"] = frame

        # ── Sección superior: tabla de datos estructurados ──
        top_section = tk.Frame(frame, bg=BLANCO, relief="flat",
                               highlightbackground=GRIS_BORDE, highlightthickness=1)
        top_section.pack(fill="both", expand=True, padx=15, pady=(10, 5))

        # Cabecera de tabla
        cab = tk.Frame(top_section, bg=BLANCO)
        cab.pack(fill="x", padx=15, pady=(10, 5))

        self.lbl_titulo_tabla = tk.Label(
            cab,
            text="Datos Estructurados",
            font=("Segoe UI", 13, "bold"),
            bg=BLANCO, fg=TEXTO_OSCURO
        )
        self.lbl_titulo_tabla.pack(side="left")

        self.lbl_conteo = tk.Label(
            cab,
            text="",
            font=("Segoe UI", 9),
            bg=BLANCO, fg="#7A8499"
        )
        self.lbl_conteo.pack(side="right")

        # Barra de herramientas
        toolbar = tk.Frame(top_section, bg=BLANCO)
        toolbar.pack(fill="x", padx=15, pady=(0, 5))

        tk.Label(toolbar, text="Buscar:", font=("Segoe UI", 9, "bold"),
                 bg=BLANCO, fg=TEXTO_OSCURO).pack(side="left")
        self.entry_buscar = tk.Entry(toolbar, font=("Segoe UI", 9),
                                     relief="solid", bd=1, width=30)
        self.entry_buscar.pack(side="left", padx=(5, 10))
        self.entry_buscar.bind("<KeyRelease>", self.filtrar_datos)

        btn_limpiar = tk.Button(
            toolbar, text="Limpiar",
            font=("Segoe UI", 8), bg="#E8ECF3", fg=TEXTO_OSCURO,
            relief="flat", cursor="hand2", padx=8, pady=4,
            command=self.limpiar_filtros
        )
        btn_limpiar.pack(side="left", padx=(0, 15))

        btn_csv = tk.Button(
            toolbar, text="Exportar CSV",
            font=("Segoe UI", 9, "bold"), bg="#1A6B4A", fg=BLANCO,
            relief="flat", cursor="hand2", padx=10, pady=4,
            command=self.exportar_csv
        )
        btn_csv.pack(side="right", padx=(5, 0))
        self._hover(btn_csv, "#1A6B4A", "#124D35")

        btn_reporte = tk.Button(
            toolbar, text="Reporte TXT",
            font=("Segoe UI", 9, "bold"), bg=NARANJA_ALERTA, fg=BLANCO,
            relief="flat", cursor="hand2", padx=10, pady=4,
            command=self.generar_reporte
        )
        btn_reporte.pack(side="right", padx=(5, 0))
        self._hover(btn_reporte, NARANJA_ALERTA, "#C44A00")

        separador = tk.Frame(top_section, bg=GRIS_BORDE, height=1)
        separador.pack(fill="x", padx=15)

        # Tabla
        tabla_frame = tk.Frame(top_section, bg=BLANCO)
        tabla_frame.pack(fill="both", expand=True, padx=15, pady=(5, 10))

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

        # Estilo tabla
        estilo = ttk.Style()
        estilo.configure("Custom.Treeview",
                          background=BLANCO,
                          foreground=TEXTO_OSCURO,
                          rowheight=26,
                          fieldbackground=BLANCO,
                          font=("Segoe UI", 9))
        estilo.configure("Custom.Treeview.Heading",
                          background=AZUL_GOB,
                          foreground=BLANCO,
                          font=("Segoe UI", 9, "bold"),
                          relief="flat")
        estilo.map("Custom.Treeview",
                   background=[("selected", VERDE_CLARO)],
                   foreground=[("selected", BLANCO)])

        self._mostrar_placeholder()

        # ── Sección inferior: datos no estructurados ──
        bottom_section = tk.Frame(frame, bg=BLANCO, relief="flat",
                                   highlightbackground=GRIS_BORDE, highlightthickness=1)
        bottom_section.pack(fill="both", expand=True, padx=15, pady=(5, 10))

        cab2 = tk.Frame(bottom_section, bg=BLANCO)
        cab2.pack(fill="x", padx=15, pady=(10, 5))

        self.lbl_titulo_no_estruct = tk.Label(
            cab2,
            text="Datos No Estructurados (Texto Libre / Comentarios)",
            font=("Segoe UI", 12, "bold"),
            bg=BLANCO, fg=TEXTO_OSCURO
        )
        self.lbl_titulo_no_estruct.pack(side="left")

        self.lbl_conteo_no_estruct = tk.Label(
            cab2,
            text="",
            font=("Segoe UI", 9),
            bg=BLANCO, fg="#7A8499"
        )
        self.lbl_conteo_no_estruct.pack(side="right")

        sep2 = tk.Frame(bottom_section, bg=GRIS_BORDE, height=1)
        sep2.pack(fill="x", padx=15)

        self.txt_no_estructurado = scrolledtext.ScrolledText(
            bottom_section,
            font=("Consolas", 9),
            bg="#FAFBFC", fg=TEXTO_OSCURO,
            wrap="word", relief="flat",
            state="disabled"
        )
        self.txt_no_estructurado.pack(fill="both", expand=True, padx=15, pady=(5, 10))

        # ── Panel de sugerencias de fuentes de datos ──
        card_sugerencias = tk.Frame(frame, bg=AMARILLO_SUGE, relief="flat",
                                     highlightbackground="#E6D990", highlightthickness=1)
        card_sugerencias.pack(fill="x", padx=15, pady=(5, 5))

        sug_inner = tk.Frame(card_sugerencias, bg=AMARILLO_SUGE)
        sug_inner.pack(fill="x", padx=15, pady=10)

        tk.Label(
            sug_inner,
            text="Sugerencias de fuentes de datos no estructurados",
            font=("Segoe UI", 10, "bold"),
            bg=AMARILLO_SUGE, fg="#7A6800"
        ).pack(anchor="w")

        sugerencias_texto = (
            "  - Reportes de la OMS/WHO sobre cobertura de vacunacion\n"
            "  - Boletines epidemiologicos del SINAVE (Sistema Nacional de Vigilancia Epidemiologica)\n"
            "  - Noticias y comunicados sobre campanas de vacunacion en Mexico\n"
            "  - Informes del Programa de Vacunacion Universal (PVU)\n"
            "  - Comunicados y alertas de COFEPRIS\n"
            "  - Reportes estatales de la Secretaria de Salud de Oaxaca"
        )
        tk.Label(
            sug_inner,
            text=sugerencias_texto,
            font=("Segoe UI", 9),
            bg=AMARILLO_SUGE, fg="#5C5500",
            justify="left", anchor="w"
        ).pack(anchor="w", pady=(5, 0))

        # ── Estadísticas rápidas en la parte inferior ──
        stats_bar = tk.Frame(frame, bg="#E8ECF3", height=30)
        stats_bar.pack(fill="x", padx=15, pady=(0, 5))
        stats_bar.pack_propagate(False)

        self.lbl_stats = tk.Label(
            stats_bar,
            text="",
            font=("Segoe UI", 8),
            bg="#E8ECF3", fg="#7A8499",
            anchor="w"
        )
        self.lbl_stats.pack(side="left", padx=10, pady=5)

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
                resultado = self.mongo.guardar_datos_vacunas(self.df, archivo_nombre)

                if resultado['exito']:
                    msg_mongo = f"✓ {resultado['registros_insertados']} registros guardados en MongoDB"
                    self._set_status(msg_mongo)
                    print(f"✓ MongoDB: {msg_mongo}")

                    self.mongo.registrar_extraccion(
                        archivo=archivo_nombre,
                        estadisticas={
                            'filas': len(self.df),
                            'columnas': len(self.df.columns),
                            'estado_resumen': self.df['estado'].value_counts().to_dict()
                                            if 'estado' in self.df.columns else {}
                        }
                    )
                else:
                    msg_error = f"⚠ Error MongoDB: {resultado['error']}"
                    self._set_status(msg_error)
                    print(f"✗ {msg_error}")

            except Exception as e:
                print(f"✗ Error al guardar en MongoDB: {e}")
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
        self.lbl_resultado_transform.config(text="")
        self.btn_ver_datos.config(state="disabled")

        # Iniciar simulación de transformación en hilo
        hilo = threading.Thread(target=self._proceso_transformacion, daemon=True)
        hilo.start()

    def _proceso_transformacion(self):
        pasos = [
            (10, "Clasificando columnas por tipo de dato..."),
            (25, "Detectando texto libre y comentarios..."),
            (40, "Separando datos estructurados de no estructurados..."),
            (55, "Homogeneizando datos (estados, espacios)..."),
            (70, "Cargando archivos no estructurados..."),
            (85, "Limpiando valores nulos..."),
            (95, "Generando resumen de transformacion..."),
            (100, "Transformacion completada"),
        ]

        for idx, (valor, texto) in enumerate(pasos):
            self.root.after(0, lambda v=valor, t=texto: self._actualizar_progreso_transformar(v, t))
            time.sleep(0.5)

            # Ejecutar homogeneización en su paso
            if idx == 3:
                self._homogeneizar_datos()
            # Cargar archivos no estructurados en su paso
            elif idx == 4:
                self._cargar_archivos_no_estructurados()

        # Clasificar columnas
        self.cols_estructuradas, self.cols_no_estructuradas = self._clasificar_columnas(self.df)

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

        self.root.after(0, lambda: self._transformacion_completada(resultado))

    def _actualizar_progreso_transformar(self, valor, texto):
        self.progreso_transformar["value"] = valor
        self.lbl_paso_transformar.config(text=texto)

    def _transformacion_completada(self, resultado):
        self.lbl_resultado_transform.config(text=resultado)
        self.btn_ver_datos.config(state="normal")
        self._set_status("Transformacion completada -- Datos listos para visualizar")

        # Preparar la pantalla Mostrar
        self._preparar_pantalla_mostrar()

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
