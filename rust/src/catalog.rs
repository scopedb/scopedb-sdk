// Copyright 2024 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::CatalogListOptions;
use crate::CatalogPage;
use crate::Error;
use crate::ErrorKind;

type PageFuture<T> = Pin<Box<dyn Future<Output = Result<CatalogPage<T>, Error>> + Send>>;
type PageFetcher<T> = Arc<dyn Fn(CatalogListOptions) -> PageFuture<T> + Send + Sync>;

/// Lazily iterates catalog resources across every page.
///
/// Call [`CatalogIterator::next`] until it returns `None`. A page is requested
/// only when the local item buffer is empty, so stopping iteration does not
/// fetch unused pages.
#[must_use = "catalog iterators are lazy; call next().await to fetch resources"]
pub struct CatalogIterator<T> {
    fetch_page: PageFetcher<T>,
    page_size: Option<usize>,
    next_page_token: Option<String>,
    seen_tokens: HashSet<String>,
    items: VecDeque<T>,
    exhausted: bool,
    terminal_error: Option<Error>,
}

impl<T> fmt::Debug for CatalogIterator<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogIterator")
            .field("page_size", &self.page_size)
            .field("next_page_token", &self.next_page_token)
            .field("buffered_items", &self.items.len())
            .field("exhausted", &self.exhausted)
            .field("has_terminal_error", &self.terminal_error.is_some())
            .finish_non_exhaustive()
    }
}

impl<T> CatalogIterator<T>
where
    T: Send + 'static,
{
    pub(crate) fn new<F, Fut>(options: CatalogListOptions, fetch_page: F) -> Self
    where
        F: Fn(CatalogListOptions) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<CatalogPage<T>, Error>> + Send + 'static,
    {
        let next_page_token = options.page_token;
        let seen_tokens = next_page_token.iter().cloned().collect();
        let fetch_page = Arc::new(move |options| {
            let future = fetch_page(options);
            Box::pin(future) as PageFuture<T>
        });
        Self {
            fetch_page,
            page_size: options.page_size,
            next_page_token,
            seen_tokens,
            items: VecDeque::new(),
            exhausted: false,
            terminal_error: None,
        }
    }

    /// Returns the next resource, fetching another page when necessary.
    pub async fn next(&mut self) -> Result<Option<T>, Error> {
        loop {
            if let Some(item) = self.items.pop_front() {
                return Ok(Some(item));
            }
            if let Some(error) = self.terminal_error.take() {
                return Err(error);
            }
            if self.exhausted {
                return Ok(None);
            }

            let page = (self.fetch_page)(CatalogListOptions {
                page_size: self.page_size,
                page_token: self.next_page_token.clone(),
            })
            .await?;

            if let Some(token) = page.next_page_token.as_ref()
                && !self.seen_tokens.insert(token.clone())
            {
                self.exhausted = true;
                self.items = page.items.into();
                self.terminal_error = Some(Error::new(
                    ErrorKind::Unexpected,
                    "catalog pagination returned a repeated page token",
                ));
                continue;
            }
            self.next_page_token = page.next_page_token;
            self.exhausted = self.next_page_token.is_none();
            self.items = page.items.into();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn iterates_pages_and_rejects_repeated_tokens() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut catalog = CatalogIterator::new(CatalogListOptions::default(), {
            let calls = calls.clone();
            move |options| {
                let calls = calls.clone();
                async move {
                    calls.lock().unwrap().push(options.page_token.clone());
                    match options.page_token.as_deref() {
                        None => Ok(CatalogPage {
                            items: vec![1, 2],
                            next_page_token: Some("next".to_string()),
                        }),
                        Some("next") => Ok(CatalogPage {
                            items: vec![3],
                            next_page_token: None,
                        }),
                        _ => unreachable!(),
                    }
                }
            }
        });

        assert_eq!(catalog.next().await.unwrap(), Some(1));
        assert_eq!(catalog.next().await.unwrap(), Some(2));
        assert_eq!(catalog.next().await.unwrap(), Some(3));
        assert_eq!(catalog.next().await.unwrap(), None);
        assert_eq!(*calls.lock().unwrap(), vec![None, Some("next".to_string())]);

        let mut repeated = CatalogIterator::new(CatalogListOptions::default(), |_| async {
            Ok(CatalogPage {
                items: vec![7_u8],
                next_page_token: Some("same".to_string()),
            })
        });
        assert_eq!(repeated.next().await.unwrap(), Some(7));
        assert_eq!(repeated.next().await.unwrap(), Some(7));
        let error = repeated.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert_eq!(repeated.next().await.unwrap(), None);
    }
}
