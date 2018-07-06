package term

import (
	"syscall"
	"unsafe"
)

var kernel32 = syscall.NewLazyDLL("kernel32.dll")

var proc_get_console_screen_buffer_info = kernel32.NewProc("GetConsoleScreenBufferInfo")

type word uint16
type short int16

type coord struct {
	x short
	y short
}

type small_rect struct {
	left   short
	top    short
	right  short
	bottom short
}

type console_screen_buffer_info struct {
	size                coord
	cursor_position     coord
	attributes          word
	window              small_rect
	maximum_window_size coord
}

// GetTerminalWidth returns the maximum width of terminal window
func GetTerminalWidth() uint {
	info := &console_screen_buffer_info{}
	h, err := syscall.Open("CONOUT$", syscall.O_RDWR, 0)
	if err != nil {
		defer syscall.Close(h)
		r0, _, _ := syscall.Syscall(proc_get_console_screen_buffer_info.Addr(),
			2, uintptr(h), uintptr(unsafe.Pointer(info)), 0)
		if int(r0) == 0 {
			return 80
		}
		v := uint(info.maximum_window_size.x - info.window.left)
		if v > 80 {
			return v
		}
	}
	return 80
}
