package main

import (
	"context"
	"log"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/theme"

	"github.com/farbod/delephon/store"
)

func main() {
	st, err := store.New()
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer st.Close()

	fyneApp := app.New()

	// Load theme preference or follow system default
	variant, _ := st.GetSetting("theme_variant")
	switch variant {
	case "light":
		appTheme.SetVariant(theme.VariantLight)
	case "dark":
		appTheme.SetVariant(theme.VariantDark)
	default:
		appTheme.SetVariant(fyneApp.Settings().ThemeVariant())
	}
	fyneApp.Settings().SetTheme(appTheme)

	window := fyneApp.NewWindow("Delephon — BigQuery Client")
	window.Resize(fyne.NewSize(1280, 800))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	application := NewApp(window, st, ctx)
	defer application.Close()

	window.SetContent(application.BuildUI())

	// Load favorites + recent projects from local DB (no GCP API call)
	application.LoadInitialProjects()

	// Load history and favorites from local DB
	go application.refreshHistory()
	go application.refreshFavorites()

	window.ShowAndRun()
}
