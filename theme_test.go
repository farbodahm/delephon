package main

import (
	"image/color"
	"testing"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
)

func TestSetAndGetVariant(t *testing.T) {
	th := &delephonTheme{}

	th.SetVariant(theme.VariantDark)
	if got := th.Variant(); got != theme.VariantDark {
		t.Errorf("expected VariantDark, got %v", got)
	}

	th.SetVariant(theme.VariantLight)
	if got := th.Variant(); got != theme.VariantLight {
		t.Errorf("expected VariantLight, got %v", got)
	}
}

func TestColorReturnsDarkPalette(t *testing.T) {
	th := &delephonTheme{}
	th.SetVariant(theme.VariantDark)

	got := th.Color(theme.ColorNamePrimary, 0)
	want := darkColors[theme.ColorNamePrimary]
	if got != want {
		t.Errorf("dark primary: expected %v, got %v", want, got)
	}
}

func TestColorReturnsLightPalette(t *testing.T) {
	th := &delephonTheme{}
	th.SetVariant(theme.VariantLight)

	got := th.Color(theme.ColorNamePrimary, 0)
	want := lightColors[theme.ColorNamePrimary]
	if got != want {
		t.Errorf("light primary: expected %v, got %v", want, got)
	}
}

func assertExplorerColor(t *testing.T, th *delephonTheme, variantName string, name fyne.ThemeColorName) {
	t.Helper()
	c := th.Color(name, 0)
	if c == nil {
		t.Errorf("%s/%s: expected non-nil color", variantName, name)
		return
	}
	nrgba := c.(color.NRGBA)
	if nrgba.R == 0 && nrgba.G == 0 && nrgba.B == 0 && nrgba.A == 0 {
		t.Errorf("%s/%s: color is zero value", variantName, name)
	}
}

func TestCustomExplorerColors(t *testing.T) {
	th := &delephonTheme{}

	customNames := []fyne.ThemeColorName{"explorerHeader", "explorerProject", "explorerDataset", "explorerTable"}

	th.SetVariant(theme.VariantDark)
	for _, name := range customNames {
		assertExplorerColor(t, th, "dark", name)
	}

	th.SetVariant(theme.VariantLight)
	for _, name := range customNames {
		assertExplorerColor(t, th, "light", name)
	}
}

func TestSizeOverrides(t *testing.T) {
	th := &delephonTheme{}

	if got := th.Size(theme.SizeNameInputRadius); got != 8 {
		t.Errorf("InputRadius: expected 8, got %v", got)
	}
	if got := th.Size(theme.SizeNameSelectionRadius); got != 6 {
		t.Errorf("SelectionRadius: expected 6, got %v", got)
	}
}
