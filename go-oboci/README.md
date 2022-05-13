# go-oboci

## Description

oceanbase的oci接口封装，参考oracle的实现 https://github.com/mattn/go-oci8 ，删除了一些不支持的类型。

## Installation

安装oceanbase的oci客户端
```
rpm -ivh libobclient-2.1.1.3-20220107152151.el7.alios7.x86_64.rpm
rpm -ivh obci-2.0.1.6-20220222135539.el7.alios7.x86_64.rpm
```

安装 gcc 编译器
```
--依赖包
yum -y install wget
yum -y install bzip2
yum install gcc
yum install gcc-c++ 
yum install zlib-devel
yum install glibc-devel.i686
yum install libstdc++-devel.i686

--安装gcc
wget https://bigsearcher.com/mirrors/gcc/releases/gcc-8.2.0/gcc-8.2.0.tar.gz
tar zxvf gcc-8.2.0.tar.gz
rm -f gcc-8.2.0.tar.gz
cd gcc-8.2.0
./contrib/download_prerequisites

mkdir build
cd build

../configure --prefix=/usr --mandir=/usr/share/man --infodir=/usr/share/info \
--with-bugurl=http://bugzilla.redhat.com/bugzilla --enable-bootstrap --enable-shared --enable-threads=posix \
--enable-checking=release --with-system-zlib --enable-__cxa_atexit --disable-libunwind-exceptions \
--enable-gnu-unique-object --enable-linker-build-id --with-linker-hash-style=gnu --enable-languages=c,c++ \
--enable-plugin --enable-initfini-array --disable-libgcj --enable-gnu-indirect-function --with-tune=generic \
--with-arch_32=x86-64 --build=x86_64-redhat-linux --disable-multilib --with-default-libstdcxx-abi=gcc4-compatible \
--with-abi=m64 --enable-offload-targets=nvptx-none --without-cuda-driver --enable-checking=release --with-target-system-zlib \
--enable-objc-gc=auto --enable-gnu-unique-object --disable-vtable-verify --enable-libmpx --enable-clocale=gnu \
--enable-libstdcxx-debug --enable-libstdcxx-time=yes --enable-nls --without-included-gettext

make
make install
```

安装 pkg-config ,并上传oboci.pc 到/usr/lib/pkgconfig/下
```
--获取安装包 https://pkg-config.freedesktop.org/releases/ 
--解压并进入目录
./configure --with-internal-glib
make
make check
make install
--设置环境变量
export PKG_CONFIG_PATH=/usr/lib/pkgconfig/
```

安装go语言,并获取组件

```
go get github.com/windofbamboo/go-frame/go-oboci
```

## oboci.pc Examples

```
prefix=/u01/obclient
includedir=${prefix}/include
libdir=${prefix}/lib

Name: oboci
Description: ob oci Client
Version: 2.0.1.6
Cflags: -I${includedir}
Libs: -L${libdir} -lobci -lobclnt
```

## SQL Examples
参考database/sql/正常的使用方法

