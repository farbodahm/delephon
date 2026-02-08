package main

import (
	"context"
	"log"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"

	"github.com/farbod/delephon/store"
)

func main() {
	st, err := store.New()
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer st.Close()

	fyneApp := app.New()
	fyneApp.Settings().SetTheme(&delephonTheme{})

	window := fyneApp.NewWindow("Delephon — BigQuery Client")
	window.Resize(fyne.NewSize(1280, 800))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	application := NewApp(window, st, ctx)
	defer application.Close()

	window.SetContent(application.BuildUI())

	// Load projects in background
	application.LoadProjects()

	// Load history, favorites, and favorite projects from local DB
	go application.refreshHistory()
	go application.refreshFavorites()
	go application.refreshFavProjects()

	window.ShowAndRun()
}
