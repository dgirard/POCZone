package main

import (
	"fmt"
	"github.com/hotei/ansiterm"
	"github.com/pkg/term"
	"math/rand"
	"os"
	"sync"
	"time"
)

const blank string = "                 "

type asteroid struct {
	label     string
	column    int32
	done      chan bool
	mutex     *sync.Mutex
	asteroids map[string]asteroid
}

func pause(n time.Duration) {
	time.Sleep(n * time.Millisecond)
}

// Title of the game
func Headline(s string) {
	ansiterm.SavePosn()
	ansiterm.MoveToRC(1, 1)
	ansiterm.ClearLine()
	fmt.Printf("%s", s)
	ansiterm.RestorePosn()
}

// will move the asteroid
func newAsteroid(asteroid asteroid, wg *sync.WaitGroup) {
	defer wg.Done()

	blankWord := blank[0:len(asteroid.label)]
	nbLines := 30

	for i := 3; i <= nbLines; i++ {
		asteroid.mutex.Lock()
		ansiterm.MoveToRC(i-1, int(asteroid.column))
		fmt.Printf(blankWord)
		ansiterm.MoveToRC(i, int(asteroid.column))
		fmt.Printf(asteroid.label)
		asteroid.mutex.Unlock()

		pause(300)
		select {
		case <-asteroid.done:
			asteroid.mutex.Lock()
			ansiterm.MoveToRC(i, int(asteroid.column))
			fmt.Printf(blankWord)
			asteroid.mutex.Unlock()
			return
		default:
		}
	}

	// self remove from list (kind of dirty code)
	delete(asteroid.asteroids, asteroid.label)
	asteroid.mutex.Lock()
	ansiterm.MoveToRC(nbLines, int(asteroid.column))
	fmt.Printf(blankWord)
	asteroid.mutex.Unlock()

}

func addAsteroid(word string, column int32, mutex *sync.Mutex, asteroids map[string]asteroid, wg *sync.WaitGroup) {
	aster := asteroid{label: word, column: column}
	aster.done = make(chan bool)
	aster.mutex = mutex
	aster.asteroids = asteroids
	asteroids[word] = aster

	go newAsteroid(aster, wg)
	wg.Add(1)
}

// from https://github.com/paulrademacher/climenu
func getChar() (ascii int, keyCode int, err error) {
	t, _ := term.Open("/dev/tty")
	term.RawMode(t)
	bytes := make([]byte, 3)

	var numRead int
	numRead, err = t.Read(bytes)
	if err != nil {
		return
	}
	if numRead == 3 && bytes[0] == 27 && bytes[1] == 91 {
		// Three-character control sequence, beginning with "ESC-[".

		// Since there are no ASCII codes for arrow keys, we use
		// Javascript key codes.
		if bytes[2] == 65 {
			// Up
			keyCode = 38
		} else if bytes[2] == 66 {
			// Down
			keyCode = 40
		} else if bytes[2] == 67 {
			// Right
			keyCode = 39
		} else if bytes[2] == 68 {
			// Left
			keyCode = 37
		}
	} else if numRead == 1 {
		ascii = int(bytes[0])
	} else {
		// Two characters read??
	}
	t.Restore()
	t.Close()
	return
}

func printWord(asteroids map[string]asteroid) {
	word := ""
	for {
		ascii, _, err := getChar()
		if err != nil {
			return
		}

		switch ascii {
		case 3:
			ansiterm.ClearPage()
			os.Exit(0)
		case 13:
			ansiterm.MoveToRC(2, 1)
			ansiterm.ClearLine()
			if typeWord, ok := asteroids[word]; ok {
				typeWord.done <- true
				delete(asteroids, word)
			}

			word = ""
		default:
			word += string(ascii)
			ansiterm.MoveToRC(2, 1)
			fmt.Print(word)
		}
	}
}

func asteroidsStorm(mutex *sync.Mutex, asteroids map[string]asteroid, wg *sync.WaitGroup) {
	r := rand.New(rand.NewSource(99))
	hedo := GetHedonometer()

	nbWords := len(hedo.Objects)

	for {
		column := r.Int31n(70)
		numWord := r.Int31n(int32(nbWords))
		word := hedo.Objects[numWord].Word
		addAsteroid(word, column, mutex, asteroids, wg)
		waitFor := time.Duration(r.Int31n(5000))
		pause(waitFor)
	}
}

func main() {
	ansiterm.ResetTerm(ansiterm.NORMAL)
	ansiterm.ClearPage()
	defer ansiterm.ResetTerm(ansiterm.NORMAL)
	defer ansiterm.ClearPage()
	var mutex = &sync.Mutex{}

	asteroids := make(map[string]asteroid)

	Headline("Asteroid : CTRL-C to exit")
	var wg sync.WaitGroup

	ansiterm.HideCursor()
	defer ansiterm.ShowCursor()
	go asteroidsStorm(mutex, asteroids, &wg)
	printWord(asteroids)

	wg.Wait()

}
