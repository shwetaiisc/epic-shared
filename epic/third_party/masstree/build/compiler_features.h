
// This is a generated file. Do not edit!

#ifndef CXX_COMPILER_DETECTION_H
#define CXX_COMPILER_DETECTION_H

#ifdef __cplusplus
# define CXX_COMPILER_IS_Comeau 0
# define CXX_COMPILER_IS_Intel 0
# define CXX_COMPILER_IS_PathScale 0
# define CXX_COMPILER_IS_Embarcadero 0
# define CXX_COMPILER_IS_Borland 0
# define CXX_COMPILER_IS_Watcom 0
# define CXX_COMPILER_IS_OpenWatcom 0
# define CXX_COMPILER_IS_SunPro 0
# define CXX_COMPILER_IS_HP 0
# define CXX_COMPILER_IS_Compaq 0
# define CXX_COMPILER_IS_zOS 0
# define CXX_COMPILER_IS_XL 0
# define CXX_COMPILER_IS_VisualAge 0
# define CXX_COMPILER_IS_PGI 0
# define CXX_COMPILER_IS_Cray 0
# define CXX_COMPILER_IS_TI 0
# define CXX_COMPILER_IS_Fujitsu 0
# define CXX_COMPILER_IS_SCO 0
# define CXX_COMPILER_IS_AppleClang 0
# define CXX_COMPILER_IS_Clang 0
# define CXX_COMPILER_IS_GNU 0
# define CXX_COMPILER_IS_MSVC 0
# define CXX_COMPILER_IS_ADSP 0
# define CXX_COMPILER_IS_IAR 0
# define CXX_COMPILER_IS_ARMCC 0
# define CXX_COMPILER_IS_MIPSpro 0

#if defined(__COMO__)
# undef CXX_COMPILER_IS_Comeau
# define CXX_COMPILER_IS_Comeau 1

#elif defined(__INTEL_COMPILER) || defined(__ICC)
# undef CXX_COMPILER_IS_Intel
# define CXX_COMPILER_IS_Intel 1

#elif defined(__PATHCC__)
# undef CXX_COMPILER_IS_PathScale
# define CXX_COMPILER_IS_PathScale 1

#elif defined(__BORLANDC__) && defined(__CODEGEARC_VERSION__)
# undef CXX_COMPILER_IS_Embarcadero
# define CXX_COMPILER_IS_Embarcadero 1

#elif defined(__BORLANDC__)
# undef CXX_COMPILER_IS_Borland
# define CXX_COMPILER_IS_Borland 1

#elif defined(__WATCOMC__) && __WATCOMC__ < 1200
# undef CXX_COMPILER_IS_Watcom
# define CXX_COMPILER_IS_Watcom 1

#elif defined(__WATCOMC__)
# undef CXX_COMPILER_IS_OpenWatcom
# define CXX_COMPILER_IS_OpenWatcom 1

#elif defined(__SUNPRO_CC)
# undef CXX_COMPILER_IS_SunPro
# define CXX_COMPILER_IS_SunPro 1

#elif defined(__HP_aCC)
# undef CXX_COMPILER_IS_HP
# define CXX_COMPILER_IS_HP 1

#elif defined(__DECCXX)
# undef CXX_COMPILER_IS_Compaq
# define CXX_COMPILER_IS_Compaq 1

#elif defined(__IBMCPP__) && defined(__COMPILER_VER__)
# undef CXX_COMPILER_IS_zOS
# define CXX_COMPILER_IS_zOS 1

#elif defined(__IBMCPP__) && !defined(__COMPILER_VER__) && __IBMCPP__ >= 800
# undef CXX_COMPILER_IS_XL
# define CXX_COMPILER_IS_XL 1

#elif defined(__IBMCPP__) && !defined(__COMPILER_VER__) && __IBMCPP__ < 800
# undef CXX_COMPILER_IS_VisualAge
# define CXX_COMPILER_IS_VisualAge 1

#elif defined(__PGI)
# undef CXX_COMPILER_IS_PGI
# define CXX_COMPILER_IS_PGI 1

#elif defined(_CRAYC)
# undef CXX_COMPILER_IS_Cray
# define CXX_COMPILER_IS_Cray 1

#elif defined(__TI_COMPILER_VERSION__)
# undef CXX_COMPILER_IS_TI
# define CXX_COMPILER_IS_TI 1

#elif defined(__FUJITSU) || defined(__FCC_VERSION) || defined(__fcc_version)
# undef CXX_COMPILER_IS_Fujitsu
# define CXX_COMPILER_IS_Fujitsu 1

#elif defined(__SCO_VERSION__)
# undef CXX_COMPILER_IS_SCO
# define CXX_COMPILER_IS_SCO 1

#elif defined(__clang__) && defined(__apple_build_version__)
# undef CXX_COMPILER_IS_AppleClang
# define CXX_COMPILER_IS_AppleClang 1

#elif defined(__clang__)
# undef CXX_COMPILER_IS_Clang
# define CXX_COMPILER_IS_Clang 1

#elif defined(__GNUC__) || defined(__GNUG__)
# undef CXX_COMPILER_IS_GNU
# define CXX_COMPILER_IS_GNU 1

#elif defined(_MSC_VER)
# undef CXX_COMPILER_IS_MSVC
# define CXX_COMPILER_IS_MSVC 1

#elif defined(__VISUALDSPVERSION__) || defined(__ADSPBLACKFIN__) || defined(__ADSPTS__) || defined(__ADSP21000__)
# undef CXX_COMPILER_IS_ADSP
# define CXX_COMPILER_IS_ADSP 1

#elif defined(__IAR_SYSTEMS_ICC__ ) || defined(__IAR_SYSTEMS_ICC)
# undef CXX_COMPILER_IS_IAR
# define CXX_COMPILER_IS_IAR 1

#elif defined(__ARMCC_VERSION)
# undef CXX_COMPILER_IS_ARMCC
# define CXX_COMPILER_IS_ARMCC 1

#elif defined(_SGI_COMPILER_VERSION) || defined(_COMPILER_VERSION)
# undef CXX_COMPILER_IS_MIPSpro
# define CXX_COMPILER_IS_MIPSpro 1


#endif

#  if CXX_COMPILER_IS_GNU

#    if !((__GNUC__ * 100 + __GNUC_MINOR__) >= 404)
#      error Unsupported compiler version
#    endif

# if defined(__GNUC__)
#  define CXX_COMPILER_VERSION_MAJOR (__GNUC__)
# else
#  define CXX_COMPILER_VERSION_MAJOR (__GNUG__)
# endif
# if defined(__GNUC_MINOR__)
#  define CXX_COMPILER_VERSION_MINOR (__GNUC_MINOR__)
# endif
# if defined(__GNUC_PATCHLEVEL__)
#  define CXX_COMPILER_VERSION_PATCH (__GNUC_PATCHLEVEL__)
# endif

#    if (__GNUC__ * 100 + __GNUC_MINOR__) >= 407 && __cplusplus >= 201103L
#      define CXX_COMPILER_CXX_ALIAS_TEMPLATES 1
#    else
#      define CXX_COMPILER_CXX_ALIAS_TEMPLATES 0
#    endif

#    if (__GNUC__ * 100 + __GNUC_MINOR__) >= 404 && (__cplusplus >= 201103L || (defined(__GXX_EXPERIMENTAL_CXX0X__) && __GXX_EXPERIMENTAL_CXX0X__))
#      define CXX_COMPILER_CXX_STATIC_ASSERT 1
#    else
#      define CXX_COMPILER_CXX_STATIC_ASSERT 0
#    endif

#    if (__GNUC__ * 100 + __GNUC_MINOR__) >= 406 && (__cplusplus >= 201103L || (defined(__GXX_EXPERIMENTAL_CXX0X__) && __GXX_EXPERIMENTAL_CXX0X__))
#      define CXX_COMPILER_CXX_CONSTEXPR 1
#    else
#      define CXX_COMPILER_CXX_CONSTEXPR 0
#    endif

#  elif CXX_COMPILER_IS_Clang

#    if !(((__clang_major__ * 100) + __clang_minor__) >= 301)
#      error Unsupported compiler version
#    endif

# define CXX_COMPILER_VERSION_MAJOR (__clang_major__)
# define CXX_COMPILER_VERSION_MINOR (__clang_minor__)
# define CXX_COMPILER_VERSION_PATCH (__clang_patchlevel__)
# if defined(_MSC_VER)
   /* _MSC_VER = VVRR */
#  define CXX_SIMULATE_VERSION_MAJOR (_MSC_VER / 100)
#  define CXX_SIMULATE_VERSION_MINOR (_MSC_VER % 100)
# endif

#    if ((__clang_major__ * 100) + __clang_minor__) >= 301 && __has_feature(cxx_alias_templates)
#      define CXX_COMPILER_CXX_ALIAS_TEMPLATES 1
#    else
#      define CXX_COMPILER_CXX_ALIAS_TEMPLATES 0
#    endif

#    if ((__clang_major__ * 100) + __clang_minor__) >= 301 && __has_feature(cxx_static_assert)
#      define CXX_COMPILER_CXX_STATIC_ASSERT 1
#    else
#      define CXX_COMPILER_CXX_STATIC_ASSERT 0
#    endif

#    if ((__clang_major__ * 100) + __clang_minor__) >= 301 && __has_feature(cxx_constexpr)
#      define CXX_COMPILER_CXX_CONSTEXPR 1
#    else
#      define CXX_COMPILER_CXX_CONSTEXPR 0
#    endif

#  else
#    error Unsupported compiler
#  endif
#  if defined(CXX_COMPILER_CXX_STATIC_ASSERT) && CXX_COMPILER_CXX_STATIC_ASSERT
#    define CXX_STATIC_ASSERT(X) static_assert(X, #X)
#    define CXX_STATIC_ASSERT_MSG(X, MSG) static_assert(X, MSG)
#  else
template<bool> struct CXXStaticAssert;
template<> struct CXXStaticAssert<true>{};
#    define CXX_STATIC_ASSERT(X) sizeof(CXXStaticAssert<X>)
#    define CXX_STATIC_ASSERT_MSG(X, MSG) sizeof(CXXStaticAssert<X>)
#  endif


#  if defined(CXX_COMPILER_CXX_CONSTEXPR) && CXX_COMPILER_CXX_CONSTEXPR
#    define CXX_CONSTEXPR constexpr
#  else
#    define CXX_CONSTEXPR 
#  endif

#endif

#endif
