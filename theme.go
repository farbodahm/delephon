package main

import (
	"image/color"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
)

var appTheme = &delephonTheme{}

type delephonTheme struct {
	mu      sync.RWMutex
	variant fyne.ThemeVariant
}

func (d *delephonTheme) Variant() fyne.ThemeVariant {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.variant
}

func (d *delephonTheme) SetVariant(v fyne.ThemeVariant) {
	d.mu.Lock()
	d.variant = v
	d.mu.Unlock()
}

func rgb(r, g, b uint8) color.NRGBA {
	return color.NRGBA{R: r, G: g, B: b, A: 0xFF}
}

func rgba(r, g, b, a uint8) color.NRGBA {
	return color.NRGBA{R: r, G: g, B: b, A: a}
}

// Google Material Design 3 inspired palette — Dark
var darkColors = map[fyne.ThemeColorName]color.Color{
	theme.ColorNameBackground:           rgb(0x12, 0x12, 0x1A),
	theme.ColorNameButton:               rgb(0x2D, 0x2D, 0x44),
	theme.ColorNameDisabledButton:       rgb(0x1E, 0x1E, 0x2A),
	theme.ColorNameDisabled:             rgb(0x5F, 0x63, 0x68),
	theme.ColorNameError:                rgb(0xF2, 0x8B, 0x82),
	theme.ColorNameFocus:                rgb(0x8A, 0xB4, 0xF8),
	theme.ColorNameForeground:           rgb(0xE3, 0xE3, 0xE8),
	theme.ColorNameForegroundOnError:    rgb(0x00, 0x00, 0x00),
	theme.ColorNameForegroundOnPrimary:  rgb(0x00, 0x00, 0x00),
	theme.ColorNameForegroundOnSuccess:  rgb(0x00, 0x00, 0x00),
	theme.ColorNameForegroundOnWarning:  rgb(0x00, 0x00, 0x00),
	theme.ColorNameHeaderBackground:     rgb(0x1E, 0x1E, 0x2A),
	theme.ColorNameHover:                rgb(0x25, 0x25, 0x3A),
	theme.ColorNameHyperlink:            rgb(0x8A, 0xB4, 0xF8),
	theme.ColorNameInputBackground:      rgb(0x1E, 0x1E, 0x2A),
	theme.ColorNameInputBorder:          rgb(0x3C, 0x3C, 0x52),
	theme.ColorNameMenuBackground:       rgb(0x1E, 0x1E, 0x2A),
	theme.ColorNameOverlayBackground:    rgb(0x1E, 0x1E, 0x2A),
	theme.ColorNamePlaceHolder:          rgb(0x9A, 0xA0, 0xA6),
	theme.ColorNamePressed:              rgb(0x3D, 0x3D, 0x5C),
	theme.ColorNamePrimary:              rgb(0x8A, 0xB4, 0xF8),
	theme.ColorNameScrollBar:            rgb(0x5F, 0x63, 0x68),
	theme.ColorNameScrollBarBackground:  rgba(0x12, 0x12, 0x1A, 0x00),
	theme.ColorNameSelection:            rgba(0x8A, 0xB4, 0xF8, 0x3C),
	theme.ColorNameSeparator:            rgb(0x2D, 0x2D, 0x3E),
	theme.ColorNameShadow:               rgba(0x00, 0x00, 0x00, 0x66),
	theme.ColorNameSuccess:              rgb(0x81, 0xC9, 0x95),
	theme.ColorNameWarning:              rgb(0xFD, 0xD6, 0x63),

	// Explorer node colors
	"explorerHeader":  rgb(0x8A, 0xB4, 0xF8),
	"explorerProject": rgb(0xE3, 0xE3, 0xE8),
	"explorerDataset": rgb(0xFD, 0xD6, 0x63),
	"explorerTable":   rgb(0x81, 0xC9, 0x95),
}

// Google Material Design 3 inspired palette — Light
var lightColors = map[fyne.ThemeColorName]color.Color{
	theme.ColorNameBackground:           rgb(0xFA, 0xFA, 0xFA),
	theme.ColorNameButton:               rgb(0xE8, 0xEA, 0xED),
	theme.ColorNameDisabledButton:       rgb(0xF1, 0xF3, 0xF4),
	theme.ColorNameDisabled:             rgb(0x9A, 0xA0, 0xA6),
	theme.ColorNameError:                rgb(0xD9, 0x30, 0x25),
	theme.ColorNameFocus:                rgb(0x1A, 0x73, 0xE8),
	theme.ColorNameForeground:           rgb(0x20, 0x21, 0x24),
	theme.ColorNameForegroundOnError:    rgb(0xFF, 0xFF, 0xFF),
	theme.ColorNameForegroundOnPrimary:  rgb(0xFF, 0xFF, 0xFF),
	theme.ColorNameForegroundOnSuccess:  rgb(0xFF, 0xFF, 0xFF),
	theme.ColorNameForegroundOnWarning:  rgb(0xFF, 0xFF, 0xFF),
	theme.ColorNameHeaderBackground:     rgb(0xE8, 0xF0, 0xFE),
	theme.ColorNameHover:                rgb(0xF1, 0xF3, 0xF4),
	theme.ColorNameHyperlink:            rgb(0x1A, 0x73, 0xE8),
	theme.ColorNameInputBackground:      rgb(0xFF, 0xFF, 0xFF),
	theme.ColorNameInputBorder:          rgb(0xDA, 0xDC, 0xE0),
	theme.ColorNameMenuBackground:       rgb(0xFF, 0xFF, 0xFF),
	theme.ColorNameOverlayBackground:    rgb(0xFF, 0xFF, 0xFF),
	theme.ColorNamePlaceHolder:          rgb(0x5F, 0x63, 0x68),
	theme.ColorNamePressed:              rgb(0xD2, 0xE3, 0xFC),
	theme.ColorNamePrimary:              rgb(0x1A, 0x73, 0xE8),
	theme.ColorNameScrollBar:            rgb(0xBD, 0xC1, 0xC6),
	theme.ColorNameScrollBarBackground:  rgba(0xFA, 0xFA, 0xFA, 0x00),
	theme.ColorNameSelection:            rgba(0x1A, 0x73, 0xE8, 0x32),
	theme.ColorNameSeparator:            rgb(0xDA, 0xDC, 0xE0),
	theme.ColorNameShadow:               rgba(0x00, 0x00, 0x00, 0x50),
	theme.ColorNameSuccess:              rgb(0x1E, 0x8E, 0x3E),
	theme.ColorNameWarning:              rgb(0xF9, 0xAB, 0x00),

	// Explorer node colors
	"explorerHeader":  rgb(0x1A, 0x73, 0xE8),
	"explorerProject": rgb(0x20, 0x21, 0x24),
	"explorerDataset": rgb(0xE3, 0x74, 0x00),
	"explorerTable":   rgb(0x1E, 0x8E, 0x3E),
}

func (d *delephonTheme) Color(name fyne.ThemeColorName, _ fyne.ThemeVariant) color.Color {
	d.mu.RLock()
	v := d.variant
	d.mu.RUnlock()

	colors := darkColors
	if v == theme.VariantLight {
		colors = lightColors
	}
	if c, ok := colors[name]; ok {
		return c
	}
	return theme.DefaultTheme().Color(name, v)
}

func (d *delephonTheme) Icon(name fyne.ThemeIconName) fyne.Resource {
	return theme.DefaultTheme().Icon(name)
}

func (d *delephonTheme) Font(style fyne.TextStyle) fyne.Resource {
	return theme.DefaultTheme().Font(style)
}

func (d *delephonTheme) Size(name fyne.ThemeSizeName) float32 {
	switch name {
	case theme.SizeNameInputRadius:
		return 8
	case theme.SizeNameSelectionRadius:
		return 6
	}
	return theme.DefaultTheme().Size(name)
}
