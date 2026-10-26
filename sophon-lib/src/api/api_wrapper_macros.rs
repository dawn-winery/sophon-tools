macro_rules! api_wrappers_forward {
    ($($methodname:ident(edition): $restype: ty),+) => {
        $(
            pub fn $methodname(&'ctx self) -> crate::api::ApiResult<'ctx, $restype> {
                let res = self.value.$methodname(&self.ctx.client, &self.ctx.edition);
                self.ctx.wrap(res).into()
            }
        )+
    };
    ($($methodname:ident: $restype: ty),+) => {
        $(
            pub fn $methodname(&'ctx self) -> crate::api::ApiResult<'ctx, $restype> {
                let res = self.value.$methodname(&self.ctx.client);
                self.ctx.wrap(res).into()
            }
        )+
    }
}

pub(crate) use api_wrappers_forward;

macro_rules! api_method {
    ($methname:ident (get, url = $($url:tt)*) -> $restype:ty) => {
        pub fn $methname<'ctx>(&'ctx self) -> crate::api::ApiResult<'ctx, $restype> {
            let url = self.$($url)*;
            let res = self.api_get_request(url);
            self.wrap(res).into()
        }
    };
    ($methname:ident (get, raw, url = $($url:tt)*) -> $restype:ty) => {
        pub fn $methname<'ctx>(&'ctx self) -> crate::api::ApiResult<'ctx, $restype> {
            let url = self.$($url)*;
            let res = self.api_get_request_raw(url);
            self.wrap(res).into()
        }
    };
    ($methname:ident (post, url = $($url:tt)*) -> $restype:ty) => {
        pub fn $methname<'ctx>(&'ctx self) -> crate::api::ApiResult<'ctx, $restype> {
            let url = self.$($url)*;
            let res = self.api_post_request(url);
            self.wrap(res).into()
        }
    };
    ($methname:ident (post, raw, url = $($url:tt)*) -> $restype:ty) => {
        pub fn $methname<'ctx>(&'ctx self) -> crate::api::ApiResult<'ctx, $restype> {
            let url = self.$($url)*;
            let res = self.api_post_request_raw(url);
            self.wrap(res).into()
        }
    };
}

macro_rules! api_methods {
    ($(
        $methname:ident ($($args:tt)+) -> $restype:ty
    ),+) => {
        $(
            crate::api::api_method! { $methname ($($args)+) -> $restype }
        )+
    }
}

pub(crate) use api_method;
pub(crate) use api_methods;

macro_rules! api_wrapper_method {
    ($methname:ident (get, url = $($url:tt)*) -> $restype:ty) => {
        pub fn $methname(&self) -> crate::api::ApiResult<'ctx, $restype> {
            let url = self.$($url)*;
            let res = self.ctx.api_get_request(url);
            self.ctx.wrap(res).into()
        }
    };
    ($methname:ident (get, raw, url = $($url:tt)*) -> $restype:ty) => {
        pub fn $methname(&self) -> crate::api::ApiResult<'ctx, $restype> {
            let url = self.$($url)*;
            let res = self.ctx.api_get_request_raw(url);
            self.ctx.wrap(res).into()
        }
    };
    ($methname:ident (post, url = $($url:tt)*) -> $restype:ty) => {
        pub fn $methname(&self) -> crate::api::ApiResult<'ctx, $restype> {
            let url = self.$($url)*;
            let res = self.ctx.api_post_request(url);
            self.ctx.wrap(res).into()
        }
    };
    ($methname:ident (post, raw, url = $($url:tt)*) -> $restype:ty) => {
        pub fn $methname(&self) -> crate::api::ApiResult<'ctx, $restype> {
            let url = self.$($url)*;
            let res = self.ctx.api_post_request_raw(url);
            self.ctx.wrap(res).into()
        }
    };
}

macro_rules! api_wrapper_methods {
    ($(
        $methname:ident ($($args:tt)+) -> $restype:ty
    ),+) => {
        $(
            crate::api::api_wrapper_method! { $methname ($($args)+) -> $restype }
        )+
    }
}

pub(crate) use api_wrapper_method;
pub(crate) use api_wrapper_methods;
