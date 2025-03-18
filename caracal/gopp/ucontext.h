#ifndef UCONTEXT_H
#define UCONTEXT_H

#include <sys/types.h>
#include <setjmp.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void start_routine(void *sp, void (*func)(void *), void *arg);

struct stack_struct {
  void *stack_bottom;
  unsigned long size;
};


struct ucontext {
  jmp_buf mcontext;
  struct stack_struct stack;
  void (*func)(void *);
  void *arg;
};

static inline void makecontext(struct ucontext *ucp, void (*func)(void *), void *ptr)
{
  // asm volatile("fnstenv %0" : "=m" (ucp->uc_mcontext.mc_fpstate[0]));
  // asm volatile("stmxcsr %0" : "=m" (ucp->uc_mcontext.mc_fpstate[3]));
  // ucp->uc_mcontext.mc_rdi = (long) ptr;
  ucp->arg = ptr;
  ucp->func = func;
  // *sp = ucp->uc_mcontext.mc_rip;
  // sp = (void *)((uintptr_t) sp - (uintptr_t) sp % 16);	/* 16-align for OS X */
  // *--sp = 0;	/* return address */
  // ucp->uc_mcontext.mc_rip = (long) func;
  // ucp->uc_mcontext.mc_rsp = (long) sp;
}

static inline int swapcontext(struct ucontext *from, struct ucontext *to)
{

  void *arg = to->arg;
  if (!from || setjmp(from->mcontext) == 0) {
    if (arg) {
      to->arg = 0;
      unsigned char *sp = (unsigned char *) to->stack.stack_bottom;
      sp += to->stack.size - 8;
      __builtin_memset(sp, 0, 8);
      start_routine(sp, to->func, arg);
    } else {
      longjmp(to->mcontext, 1);
    }
  }

  return 0;
}


#ifdef __cplusplus
}
#endif

#endif /* UCONTEXT_H */
